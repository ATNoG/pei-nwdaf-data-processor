import logging
import asyncio
from collections import defaultdict
from typing import Callable
from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy, SkipStrategy, KNNStrategy
from src.window_buffer import CellWindowBuffer
import httpx

logger = logging.getLogger("TimeWindowManager")


class TimeWindowManager:
    """
    Manages event-time aligned windows for measurements grouped by cell.
    Window lifecycle is driven by watermark advancement.

    Supports both tumbling and sliding windows via the slide_interval parameter.
    When slide_interval == window_size, behavior is identical to tumbling windows.
    """

    def __init__(
        self,
        window_size: int,
        storage_struct,
        on_window_complete: Callable | None = None,                  # callback for completed windows
        processing_profiles: list[ProcessingProfile] | None = None,
        empty_window_strategy: EmptyWindowStrategy | None = None,
        slide_interval: int | None = None,
    ):
        self.window_size = window_size
        self.slide_interval = slide_interval if slide_interval is not None else window_size

        if self.slide_interval <= 0 or self.slide_interval > self.window_size:
            raise ValueError(
                f"slide_interval must be in (0, window_size]. "
                f"Got slide_interval={self.slide_interval}, window_size={self.window_size}"
            )

        self.on_window_complete = on_window_complete or print
        self.processing_profiles = processing_profiles or []
        self.empty_window_strategy = empty_window_strategy or SkipStrategy()

        self.storage_struct = storage_struct

        self.watermark: int = 0
        self._last_processed: dict[int, dict] = {}  # Track last processed window per cell
        self._can_change_watermark = True

        # Per-cell window buffers for sliding window data
        self._buffers: dict[int, CellWindowBuffer] = {}

        # History buffer for KNN strategy (shared across all profiles)
        self._history_buffer: dict[str, list[dict]] = defaultdict(list)

        # If using KNN strategy, inject the shared history buffer
        if isinstance(self.empty_window_strategy, KNNStrategy):
            self.empty_window_strategy.history_buffer = self._history_buffer

    def _get_or_create_buffer(self, cell_index: int) -> CellWindowBuffer:
        if cell_index not in self._buffers:
            self._buffers[cell_index] = CellWindowBuffer(time_field="timestamp")
        return self._buffers[cell_index]

    def set_initial_watermark(self, watermark: int) -> None:
        if self._can_change_watermark:
            self.watermark = watermark
            self._can_change_watermark = False

    async def advance_watermark(self, watermark_time: int):
        """
        Advances watermark monotonically and finalizes all windows
        whose end <= watermark.
        """

        if watermark_time <= self.watermark:
            logger.error(f"Cannot move watermark backwards. {self.watermark} >{watermark_time}")
            return
        self._can_change_watermark = False

        # The slice to fetch is [old_watermark, new_watermark) — only new data
        fetch_start = self.watermark
        fetch_end = watermark_time

        # The full window to process is [watermark_time - window_size, watermark_time)
        window_start = max(0, watermark_time - self.window_size)
        window_end = watermark_time

        # Fetch cells asynchronously
        cells = await self._fetch_cells()
        if not cells:
            # no cell registered
            return

        # Prune buffers for cells that no longer exist
        active_cell_set = set(cells)
        stale_cells = [c for c in self._buffers if c not in active_cell_set]
        for c in stale_cells:
            del self._buffers[c]

        # Process all cells in parallel
        tasks = [
            self._process_cell_window(cell, fetch_start, fetch_end, window_start, window_end)
            for cell in cells
        ]
        await asyncio.gather(*tasks)

        # update watermark
        self.watermark = watermark_time

    async def _fetch_slice(self, cell_index: int, start_time: int, end_time: int) -> list[dict] | None:
        """
        Fetch raw data for a single cell in the time range [start_time, end_time).
        Returns list of records.
        """
        URL = self.storage_struct.url + self.storage_struct.endpoint.raw

        fetch: bool = True
        slice_data: list = []
        batch_number = 1

        async with httpx.AsyncClient() as client:
            while fetch:
                # prepare params
                params = {
                    "batch_number": batch_number,
                    "cell_index": cell_index,
                    "start_time": start_time,
                    "end_time": end_time
                }
                try:
                    response = await client.get(f"{URL}", params=params, timeout=30.0)
                    response.raise_for_status()

                    result = response.json()

                    data = result.get("data", None)
                    if data is None:
                        logger.warning(f"[ABORTING] Bad response from storage API: {result}")
                        return

                    fetch = result.get("has_next", False)
                    batch_number += 1

                    slice_data.extend(data)

                except httpx.HTTPError as e:
                    logger.error(f"Failed to fetch data for cell {cell_index} batch {batch_number}: {e}")
                    return

        return slice_data

    async def _process_cell_window(self, cell_index: int, fetch_start: int, fetch_end: int,
                                   window_start: int, window_end: int):
        """
        For one cell: fetch new data slice, update window buffer, process full window.
        """
        # Step 1: Fetch the new slice from the API
        new_data = await self._fetch_slice(cell_index, fetch_start, fetch_end)
        if new_data is None:
            # API error — skip this cell this tick, but preserve existing buffer
            return

        # Step 2: Append new data to the window buffer
        buf = self._get_or_create_buffer(cell_index)
        buf.append_slice(new_data)

        # Step 3: Evict data older than window_start
        buf.evict_before(window_start)

        # Step 4: Get the full buffered window
        window_data = buf.get_all()

        # Step 5: Process through profiles
        is_empty: bool = len(window_data) == 0
        for profile in self.processing_profiles:

            if is_empty:
                # use profile empty window handling
                last_processed = self._last_processed.get(cell_index)
                data = profile.handle_empty_window(
                    cell_id=str(cell_index),
                    window_start=window_start,
                    window_end=window_end,
                    strategy=self.empty_window_strategy,
                    last_processed=last_processed)
            else:
                data = profile.process(window_data)

            if data is None:
                continue

            data["window_start"] = window_start
            data["window_duration_seconds"] = self.window_size
            data["window_end"] = window_end

            # Store as last processed for this cell
            self._last_processed[cell_index] = data.copy()

            # Add to KNN history buffer if not an empty window
            # This happens AFTER processing so KNN has access to aggregated stats
            if not is_empty and isinstance(self.empty_window_strategy, KNNStrategy):
                self.empty_window_strategy.add_to_history(str(cell_index), data)

            self.on_window_complete(data)

    async def _fetch_cells(self) -> list[int]:
        """Fetch cells from api"""
        URL = self.storage_struct.url + self.storage_struct.endpoint.cell

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{URL}", timeout=30.0)
                response.raise_for_status()

                result = response.json()

                if not isinstance(result, list):
                    logger.warning(f"[ABORTING] Unexpected response format: {result}")
                    return []

                return result

        except httpx.HTTPError:
            logger.error("Failed to fetch cells")
            return []
