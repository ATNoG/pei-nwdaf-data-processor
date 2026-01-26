import logging
import asyncio
from collections import defaultdict
from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy, SkipStrategy, KNNStrategy
import httpx

logger = logging.getLogger("TimeWindowManager")


class TimeWindowManager:
    """
    Manages event-time aligned windows for measurements grouped by cell.
    Window lifecycle is driven by watermark advancement.
    """

    def __init__(
        self,
        window_size: int,
        storage_struct,
        on_window_complete: callable | None = None,                  # callback for completed windows
        processing_profiles: list[ProcessingProfile] | None = None,
        empty_window_strategy: EmptyWindowStrategy | None = None,
    ):
        self.window_size = window_size
        self.on_window_complete = on_window_complete or print
        self.processing_profiles = processing_profiles or []
        self.empty_window_strategy = empty_window_strategy or SkipStrategy()

        self.storage_struct = storage_struct

        self.watermark: int = 0
        self._last_processed: dict[int, dict] = {}  # Track last processed window per cell
        self._can_change_watermark = True

        # History buffer for KNN strategy (shared across all profiles)
        self._history_buffer: dict[str, list[dict]] = defaultdict(list)

        # If using KNN strategy, inject the shared history buffer
        if isinstance(self.empty_window_strategy, KNNStrategy):
            self.empty_window_strategy.history_buffer = self._history_buffer

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

        start_time = self.watermark
        end_time = watermark_time

        # Fetch cells asynchronously
        cells = await self._fetch_cells()
        if not cells:
            # no cell registered
            return

        # Process all cells in parallel
        tasks = [self._make_window(cell, start_time, end_time) for cell in cells]
        await asyncio.gather(*tasks)

        # update watermark
        self.watermark = watermark_time

    async def _make_window(self, cell_index: int, start_time: int, end_time: int):

        URL = self.storage_struct.url + self.storage_struct.endpoint.raw

        fetch: bool = True
        window_data: list = []
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

                    window_data.extend(data)

                except httpx.HTTPError as e:
                    logger.error(f"Failed to fetch data for cell {cell_index} batch {batch_number}: {e}")
                    return

        # make windows
        is_empty: bool = len(window_data) == 0
        for profile in self.processing_profiles:

            if is_empty:
                # use profile empty window handling
                last_processed = self._last_processed.get(cell_index)
                data = profile.handle_empty_window(
                    cell_id=str(cell_index),
                    window_start=start_time,
                    window_end=end_time,
                    strategy=self.empty_window_strategy,
                    last_processed=last_processed)
            else:
                data = profile.process(window_data)

            if data is None:
                continue

            data["window_start"] = start_time
            data["window_duration_seconds"] = self.window_size
            data["window_end"]   = end_time

            # Store as last processed for this cell (only if it's not an empty window)
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
