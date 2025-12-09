from typing import Callable
import logging
from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy, SkipStrategy

logger = logging.getLogger("TimeWindowManager")


class TimeWindowManager:
    """
    Manages event-time aligned windows for measurements grouped by cell.
    Window lifecycle is driven by watermark advancement.
    """

    def __init__(
        self,
        window_size: int,                                            # window duration in seconds
        on_window_complete: Callable | None = None,                  # callback for completed windows
        processing_profiles: list[ProcessingProfile] | None = None,
        empty_window_strategy: EmptyWindowStrategy | None = None,
        allowed_lateness_seconds: int = 5,                           # allowed out-of-order lateness
    ):
        self.window_size = window_size
        self.on_window_complete = on_window_complete or print
        self.processing_profiles = processing_profiles or []
        self.empty_window_strategy = empty_window_strategy or SkipStrategy
        self.allowed_lateness_seconds = allowed_lateness_seconds

        self.watermark: int = 0
        self._windows: dict[str, dict[int, list[dict]]] = {}

    def advance_watermark(self, watermark_time: int):
        """
        Advances watermark monotonically and finalizes all windows
        whose end <= watermark.
        """

        if watermark_time <= self.watermark:
            logger.error(f"Cannot move watermark backwards. {self.watermark} >{watermark_time}")
            return

        # update watermark
        self.watermark = watermark_time

        # close windows
        self.check()

    def check(self):
        """
        closes necessary windows
        """

        for cell_id, windows in self._windows.items():
            for start_time in list(windows.keys()):
                window_end = start_time + self.window_size
                if window_end + self.allowed_lateness_seconds <= self.watermark:
                    self._close_window(cell_id, start_time)



    def add_measurement(self, measurement: dict) -> None:
        """
        Adds a measurement to the appropriate event-time window.
        Late data (earlier than watermark) is dropped or logged.
        """

        try:
            # Extract cell identifier and timestamp
            cell_id = measurement.get('cell_index')
            timestamp = int(float(measurement.get('timestamp')))

            if cell_id is None:
                return
            if timestamp < self.watermark - self.allowed_lateness_seconds:
                logger.warning(f"Dropping measurement collected at {timestamp} for cell [{cell_id}]")
                return

            window_start = self._get_window_start(timestamp)
            if cell_id not in self._windows:
                self._windows[cell_id] = {}
            if window_start not in self._windows[cell_id]:
                self._windows[cell_id][window_start] = []

            # Add measurement to the appropriate window
            self._windows[cell_id][window_start].append(measurement)

        except Exception as e:
            logger.error(f"Failed to add measurement {e}")

    def _close_window(self, cell_id: str, window_start: int):
        """
        Finalizes a concrete window.
        Applies processing profiles and invokes callback.
        """
        target_measurements:list|None = self._windows.get(cell_id,{}).get(window_start,None)
        if target_measurements is None:
            logger.error(f"Window not found for cell [{cell_id}] at {window_start}")
            return

        is_empty:bool = len(target_measurements) == 0
        profile:ProcessingProfile
        for profile in self.processing_profiles:
            data:dict|None
            if is_empty:
                # process empty
                data = profile.handle_empty_window(
                    cell_id=cell_id,
                    window_start=window_start,
                    window_end=window_start+self.window_size,
                    strategy=self.empty_window_strategy)
            else:
                data = profile.process(target_measurements)

            if data is None:
                continue

            data["window_start"] = window_start
            data["window_end"]   = window_start+self.window_size

            self.on_window_complete(data)

        del self._windows[cell_id][window_start]

        next_window = window_start + self.window_size
        self._windows.setdefault(cell_id, {}).setdefault(next_window, [])


    def _get_window_start(self, timestamp: int) -> int:
        """
        Computes aligned window start for a given event timestamp.
        """
        return (timestamp // self.window_size) * self.window_size
