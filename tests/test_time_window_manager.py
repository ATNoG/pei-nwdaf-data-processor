import pytest
from typing import override

from src.time_window_manager import TimeWindowManager
from src.profiles.processing_profile import ProcessingProfile

class DummyProcessingProfile(ProcessingProfile):
    """
    Minimal processing profile used for unit tests.
    """

    @classmethod
    @override
    def process(cls,data):
        return {
            "count": len(data),
            "measurements": list(data),
        }

    @classmethod
    @override
    def get_empty_window_context(cls, cell_id, last_processed=None):
        """Provide minimal context for empty window handling in tests."""
        return {}

    @classmethod
    @override
    def handle_empty_window(cls,cell_id, window_start, window_end, strategy):
        return {
            "cell_id": cell_id,
            "window_start": window_start,
            "window_end": window_end,
            "empty": True,
        }

def test_window_start():
    manager = TimeWindowManager(10)

    assert manager._get_window_start(12) == 10
    assert manager._get_window_start(39) == 30


def test_add_measurement_and_close():
    outputs = []
    manager = TimeWindowManager(10, on_window_complete=lambda data: outputs.append(data),
                                processing_profiles=[DummyProcessingProfile()])

    # add a measurement
    manager.add_measurement({"cell_index": "cell1", "timestamp": 5})
    manager.add_measurement({"cell_index": "cell1", "timestamp": 7})

    # nothing closed yet
    assert outputs == []

    # advance watermark to close past window
    manager.advance_watermark(15)

    assert len(outputs) == 1
    assert outputs[0]["count"] == 2
    assert outputs[0]["measurements"][0]["timestamp"] == 5



def test_empty_window():
    outputs = []
    manager = TimeWindowManager(10, on_window_complete=lambda data: outputs.append(data),
                                processing_profiles=[DummyProcessingProfile()])

    # create empty window for known cell
    manager.add_measurement({"cell_index": "cell1", "timestamp": 5})
    manager.advance_watermark(15)  # closes first window

    # next window [10,20) has no measurement, but we trigger its creation
    manager.advance_watermark(25)

    # check last emitted is empty
    last_output = outputs[-1]
    assert last_output.get("empty") is True
    assert last_output["cell_id"] == "cell1"
    assert last_output["window_start"] == 10


def test_late_measurement():
    outputs = []
    manager = TimeWindowManager(10, on_window_complete=lambda data: outputs.append(data),
                                processing_profiles=[DummyProcessingProfile()],
                                allowed_lateness_seconds=5)

    # add measurement that is too late
    manager.advance_watermark(20)
    manager.add_measurement({"cell_index": "cell1", "timestamp": 10})
    assert outputs == []  # should be dropped

    # add measurement within allowed lateness
    manager.add_measurement({"cell_index": "cell1", "timestamp": 16})
    manager.advance_watermark(25)
    assert len(outputs) == 1
    assert outputs[0]["count"] == 1
