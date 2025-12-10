import pytest

from src.profiles.latency_profile import LatencyProfile
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy, ForwardFillStrategy


def test_skip_strategy_returns_none():
	# SkipStrategy should cause the profile to return None for empty windows
	strategy = SkipStrategy()
	res = LatencyProfile.handle_empty_window(cell_id="cellA", window_start=100, window_end=110, strategy=strategy)
	assert res is None


def test_zero_fill_strategy_returns_zeroed_structure():
	strategy = ZeroFillStrategy()
	res = LatencyProfile.handle_empty_window(cell_id="cellB", window_start=200, window_end=210, strategy=strategy)

	# ZeroFillStrategy requires fields in the context; LatencyProfile provides them.
	assert isinstance(res, dict)
	assert res["cell_index"] == "cellB"
	assert res["sample_count"] == 0
	assert res["start_time"] == 200
	assert res["end_time"] == 210
	# Each field should be present and have samples==0
	for field in LatencyProfile.FIELDS:
		assert field in res
		assert isinstance(res[field], dict)
		assert res[field]["samples"] == 0
		assert res[field]["min"] is None


def test_forward_fill_strategy_uses_last_processed():
	# Create a sample processed result using LatencyProfile.process
	sample_data = [
		{"cell_index": "cellC", "timestamp": 300, "rsrp": -80, "sinr": 20.0, "rsrq": -10, "mean_latency": 15, "cqi": 10, "network": "net1"}
	]

	last_processed = LatencyProfile.process(sample_data)
	assert last_processed is not None

	strategy = ForwardFillStrategy()
	# Provide last_processed explicitly to the handler
	res = LatencyProfile.handle_empty_window(cell_id="cellC", window_start=310, window_end=320, strategy=strategy, last_processed=last_processed)

	# Forward filled result should reuse last_processed and update timestamps
	assert isinstance(res, dict)
	assert res.get("cell_index") == last_processed.get("cell_index")
	assert res.get("start_time") == 310
	assert res.get("end_time") == 320
	assert res.get("sample_count") == 0
	assert res.get("forward_filled", False) is True
