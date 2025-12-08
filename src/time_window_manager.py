from collections import defaultdict
from typing import Dict, List, Callable, Optional, Type
import logging

from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy, SkipStrategy

logger = logging.getLogger(__name__)


class TimeWindowManager:
    """
    Manages time-aligned windows for measurements grouped by cell.
    All cells share the same window boundaries (e.g., 10:00-11:00, 11:00-12:00).
    """

    def __init__(
        self,
        window_duration_seconds: int = 10,
        on_window_complete: Optional[Callable] = None,
        processing_profile: Optional[Type[ProcessingProfile]] = None,
        empty_window_strategy: Optional[EmptyWindowStrategy] = None
    ):

        self.window_duration = window_duration_seconds
        self.on_window_complete = on_window_complete
        self.processing_profile = processing_profile
        self.empty_window_strategy = empty_window_strategy if empty_window_strategy else SkipStrategy()

        # Storage structure: {cell_id: {window_start: {'measurements': [...]}}}
        self.windows: Dict[str, Dict[int, Dict]] = defaultdict(lambda: defaultdict(lambda: {'measurements': []}))

        # Track the latest completed window boundary for each cell
        self.last_completed_window: Dict[str, int] = {}
        
        # Track the last processed result for each cell (for forward-fill strategy)
        self.last_processed_result: Dict[str, Dict] = {}

    def add_measurement(self, measurement: dict) -> Optional[List[Dict]]:
        """
        Add a measurement to the appropriate time-aligned window.
        Returns a list of completed windows if any were closed, None otherwise
        """
        try:
            # Extract cell identifier and timestamp
            cell_id = measurement.get('cell_index')
            timestamp = int(float(measurement.get('timestamp')))

            if cell_id is None:
                return None

            window_start = self._get_window_start(timestamp)

            # Add measurement to the appropriate window
            self.windows[cell_id][window_start]['measurements'].append(measurement)

            # Check if we should close any completed windows for this cell
            completed_windows = self._check_and_close_windows(cell_id, timestamp)

            return completed_windows if completed_windows else None

        except Exception as e:
            logger.error(f"Error adding measurement: {e}")
            return None

    def _get_window_start(self, timestamp: int) -> int:
        """Calculate the start of the time-aligned window for a given timestamp."""
        return (timestamp // self.window_duration) * self.window_duration

    def _check_and_close_windows(self, cell_id: str, current_timestamp: int) -> List[Dict]:
        """
        Check if any windows should be closed based on the current timestamp.
        A window is closed if its end time is <= current_timestamp.
        Also handles empty windows between the last completed and current window.
        """
        completed_windows = []
        current_window_start = self._get_window_start(current_timestamp)
        cell_windows = self.windows[cell_id]

        # Find all windows that should be closed (end time < current window start)
        windows_to_close = [
            window_start for window_start in cell_windows.keys()
            if window_start < current_window_start
        ]

        # Handle empty windows gaps
        completed_windows.extend(self._process_empty_windows(cell_id, current_window_start))

        # Close the identified windows with data
        for window_start in windows_to_close:
            window_data = cell_windows[window_start]
            if window_data['measurements']:
                window_end = window_start + self.window_duration
                completed_window = self._close_window(cell_id, window_start, window_end, window_data)
                completed_windows.append(completed_window)

            # Remove the closed window from storage
            del cell_windows[window_start]

        return completed_windows

    def _close_window(self, cell_id: str, window_start: int, window_end: int, window_data: dict) -> Dict:
        """
        Close a window and prepare its data for processing.
        If a processing_profile is configured, processes the measurements before callback.

        Returns:
            Dictionary containing window metadata and processed/raw measurements
        """

        try:
            processed_result = self.processing_profile.process(measurements)
            # Store successful result for potential forward-fill
            if processed_result:
                self.last_processed_result[cell_id] = processed_result
        except Exception as e:
            logger.error(f"Error processing window for cell {cell_id}: {e}")
            processed_result = None

        completed_window = {
            'cell_id': cell_id,
            'window_start': window_start,
            'window_end': window_end,
            'measurement_count': len(measurements),
            'measurements': measurements.copy() if not self.processing_profile else None,
            'processed': processed_result
        }

        self.last_completed_window[cell_id] = window_start

        logger.info(
            f"Window completed for cell {cell_id}: "
            f"{completed_window['measurement_count']} measurements "
            f"from {completed_window['window_start']} to {completed_window['window_end']}"
        )

        # Call the callback if provided
        if self.on_window_complete:
            try:
                self.on_window_complete(
                    cell_id,
                    processed_result if processed_result else measurements,
                    completed_window['window_start'],
                    completed_window['window_end']
                )
            except Exception as e:
                logger.error(f"Error in window complete callback: {e}")

        return completed_window

    def _process_empty_windows(self, cell_id: str, current_window_start: int) -> List[Dict]:
        """Process empty windows between the last completed and current window"""
        completed_windows = []
        last_completed = self.last_completed_window.get(cell_id, None)

        if last_completed is None:
            return []

        cell_windows = self.windows[cell_id]
        next_window = last_completed + self.window_duration

        while next_window < current_window_start:
    def _handle_empty_window(self, cell_id: str, window_start: int) -> List[Dict]:
        """Handle an empty window using the configured strategy and processing profile"""

        window_end = window_start + self.window_duration

        try:
            last_processed = self.last_processed_result.get(cell_id)
            empty_result = self.processing_profile.handle_empty_window(
                cell_id, window_start, window_end, self.empty_window_strategy, last_processed
            )

            if empty_result is None:
                return []

            self.last_completed_window[cell_id] = window_start

            logger.info(f"Handled empty window for cell {cell_id}: {window_start}-{window_end}")

            # Call the callback if provided
            if self.on_window_complete:
                try:
                    self.on_window_complete(cell_id, empty_result, window_start, window_end)
                except Exception as e:
                    logger.error(f"Error in window complete callback for empty window: {e}")

            return [{
                'cell_id': cell_id,
                'window_start': window_start,
                'window_end': window_end,
                'measurement_count': 0,
                'processed': empty_result,
                'is_empty': True
            }]

        except Exception as e:
            logger.error(f"Error handling empty window for cell {cell_id}: {e}")
            return [{'processed': empty_result,
                'is_empty': True
            }]

        except Exception as e:
            logger.error(f"Error handling empty window for cell {cell_id}: {e}")
            return []

    def force_close_all_windows(self) -> List[Dict]:
        """Force close all active windows (useful for shutdown or manual flush)"""
        completed_windows = []

        for cell_id, cell_windows in self.windows.items():
            for window_start, window_data in cell_windows.items():
                if window_data['measurements']:
                    window_end = window_start + self.window_duration
                    completed_window = self._close_window(cell_id, window_start, window_end, window_data)
                    completed_windows.append(completed_window)

        # Clear all windows
        self.windows.clear()

        logger.info(f"Forced close of {len(completed_windows)} windows")
        return completed_windows
