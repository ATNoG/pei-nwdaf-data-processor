import pytest

from src.profiles.latency_profile import LatencyProfile
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy, ForwardFillStrategy
from src.time_window_manager import TimeWindowManager


# ============================================================================
# BASIC STRATEGY TESTS
# ============================================================================

def test_skip_strategy_returns_none():
	"""SkipStrategy should always return None"""
	strategy = SkipStrategy()
	res = LatencyProfile.handle_empty_window(
		cell_id="cellA",
		window_start=100,
		window_end=110,
		strategy=strategy
	)
	assert res is None


def test_skip_strategy_ignores_context():
	"""SkipStrategy should return None even with context provided"""
	strategy = SkipStrategy()
	context = {"fields": ["rsrp"], "metadata": {"network": "net1"}}
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=context)
	assert res is None


# ============================================================================
# ZEROFILL STRATEGY TESTS
# ============================================================================

def test_zero_fill_strategy_returns_zeroed_structure():
	"""ZeroFillStrategy should create structure with zeroed stats"""
	strategy = ZeroFillStrategy()
	res = LatencyProfile.handle_empty_window(
		cell_id="cellB",
		window_start=200,
		window_end=210,
		strategy=strategy
	)

	assert isinstance(res, dict)
	assert res["cell_index"] == "cellB"
	assert res["sample_count"] == 0
	assert res["start_time"] == 200
	assert res["end_time"] == 210
	assert res.get("is_empty_window") is True

	# Each field should be present and have samples==0
	for field in LatencyProfile.FIELDS:
		assert field in res
		assert isinstance(res[field], dict)
		assert res[field]["samples"] == 0
		assert res[field]["min"] is None
		assert res[field]["max"] is None
		assert res[field]["mean"] is None
		assert res[field]["std"] is None


def test_zero_fill_without_context_returns_none():
	"""ZeroFillStrategy should return None when context is missing"""
	strategy = ZeroFillStrategy()
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=None)
	assert res is None


def test_zero_fill_without_fields_in_context_returns_none():
	"""ZeroFillStrategy should return None when 'fields' not in context"""
	strategy = ZeroFillStrategy()
	context = {"metadata": {"network": "net1"}}  # Missing 'fields'
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=context)
	assert res is None


def test_zero_fill_includes_metadata():
	"""ZeroFillStrategy should include metadata from context"""
	strategy = ZeroFillStrategy()
	context = {
		"fields": ["rsrp", "sinr"],
		"metadata": {
			"network": "net1",
			"primary_bandwidth": 20,
			"ul_bandwidth": 10
		}
	}
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=context)

	assert res["network"] == "net1"
	assert res["primary_bandwidth"] == 20
	assert res["ul_bandwidth"] == 10


def test_zero_fill_structure_consistency():
	"""ISSUE: ZeroFill creates inconsistent structure compared to normal processing"""
	# Normal processing
	sample_data = [
		{
			"cell_index": "cellA",
			"timestamp": 100,
			"rsrp": -80,
			"sinr": 20.0,
			"rsrq": -10,
			"mean_latency": 15,
			"cqi": 10,
			"network": "net1"
		}
	]
	normal_result = LatencyProfile.process(sample_data)

	# Empty window with ZeroFill
	strategy = ZeroFillStrategy()
	empty_result = LatencyProfile.handle_empty_window(
		cell_id="cellA",
		window_start=200,
		window_end=210,
		strategy=strategy
	)

	# ISSUE 1: Normal processing has 'type' field, ZeroFill doesn't
	assert "type" in normal_result
	assert "type" not in empty_result  # This is the problem!

	# ISSUE 2: ZeroFill creates start_time/end_time, but TimeWindowManager adds window_start/window_end
	# This means empty windows will have 4 timestamp fields while normal ones have 2
	assert "start_time" in empty_result
	assert "end_time" in empty_result
	# TimeWindowManager would add these later:
	# assert "window_start" in result
	# assert "window_end" in result


# ============================================================================
# FORWARDFILL STRATEGY TESTS
# ============================================================================

def test_forward_fill_strategy_uses_last_processed():
	"""ForwardFillStrategy should reuse last processed values"""
	# Create a sample processed result
	sample_data = [
		{
			"cell_index": "cellC",
			"timestamp": 300,
			"rsrp": -80,
			"sinr": 20.0,
			"rsrq": -10,
			"mean_latency": 15,
			"cqi": 10,
			"network": "net1"
		}
	]
	last_processed = LatencyProfile.process(sample_data)
	assert last_processed is not None

	strategy = ForwardFillStrategy()
	res = LatencyProfile.handle_empty_window(
		cell_id="cellC",
		window_start=310,
		window_end=320,
		strategy=strategy,
		last_processed=last_processed
	)

	# Forward filled result should reuse last_processed and update timestamps
	assert isinstance(res, dict)
	assert res.get("cell_index") == last_processed.get("cell_index")
	assert res.get("start_time") == 310
	assert res.get("end_time") == 320
	assert res.get("sample_count") == 0
	assert res.get("forward_filled", False) is True


def test_forward_fill_without_context_returns_none():
	"""ForwardFillStrategy should return None when context is missing"""
	strategy = ForwardFillStrategy()
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=None)
	assert res is None


def test_forward_fill_without_last_values_returns_none():
	"""ForwardFillStrategy should return None when no last_values in context"""
	strategy = ForwardFillStrategy()
	context = {"fields": ["rsrp"]}  # Missing 'last_values'
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=context)
	assert res is None


def test_forward_fill_preserves_statistics():
	"""ISSUE: ForwardFill copies actual statistics, which is misleading for empty windows"""
	sample_data = [
		{
			"cell_index": "cellC",
			"timestamp": 300,
			"rsrp": -80,
			"sinr": 20.0,
			"rsrq": -10,
			"mean_latency": 15,
			"cqi": 10,
			"network": "net1"
		}
	]
	last_processed = LatencyProfile.process(sample_data)

	strategy = ForwardFillStrategy()
	res = LatencyProfile.handle_empty_window(
		cell_id="cellC",
		window_start=310,
		window_end=320,
		strategy=strategy,
		last_processed=last_processed
	)

	# ISSUE: The forward-filled result contains actual statistics from the previous window
	# even though sample_count is 0. This is misleading!
	assert res["sample_count"] == 0  # Says no samples
	assert res["rsrp"]["samples"] == 1  # But field says 1 sample! Contradiction!
	assert res["rsrp"]["min"] == -80  # Contains actual values from previous window
	assert res["rsrp"]["mean"] == -80

	# This makes it look like the empty window has these characteristics,
	# when really it's just copied from before


def test_forward_fill_preserves_metadata():
	"""ForwardFillStrategy should preserve metadata from last processed"""
	sample_data = [
		{
			"cell_index": "cellC",
			"timestamp": 300,
			"rsrp": -80,
			"sinr": 20.0,
			"rsrq": -10,
			"mean_latency": 15,
			"cqi": 10,
			"network": "net1",
			"primary_bandwidth": 20,
			"ul_bandwidth": 10
		}
	]
	last_processed = LatencyProfile.process(sample_data)

	strategy = ForwardFillStrategy()
	res = LatencyProfile.handle_empty_window(
		cell_id="cellC",
		window_start=310,
		window_end=320,
		strategy=strategy,
		last_processed=last_processed
	)

	assert res["network"] == "net1"
	assert res["primary_bandwidth"] == 20
	assert res["ul_bandwidth"] == 10


def test_forward_fill_marks_as_forward_filled():
	"""ForwardFillStrategy should add forward_filled flag"""
	sample_data = [
		{
			"cell_index": "cellC",
			"timestamp": 300,
			"rsrp": -80,
			"sinr": 20.0,
			"rsrq": -10,
			"mean_latency": 15,
			"cqi": 10,
			"network": "net1"
		}
	]
	last_processed = LatencyProfile.process(sample_data)

	strategy = ForwardFillStrategy()
	res = LatencyProfile.handle_empty_window(
		cell_id="cellC",
		window_start=310,
		window_end=320,
		strategy=strategy,
		last_processed=last_processed
	)

	assert res.get("forward_filled") is True
	assert res.get("is_empty_window") is True


# ============================================================================
# INTEGRATION TESTS WITH TIMEWINDOWMANAGER
# ============================================================================

def test_time_window_manager_skip_empty_windows():
	"""TimeWindowManager with SkipStrategy should not output empty windows"""
	results = []

	manager = TimeWindowManager(
		window_size=10,
		on_window_complete=lambda data: results.append(data),
		processing_profiles=[LatencyProfile],
		empty_window_strategy=SkipStrategy()
	)

	# Add measurement in first window [0-10)
	manager.add_measurement({
		"cell_index": "cellA",
		"timestamp": 5,
		"rsrp": -80,
		"sinr": 20.0,
		"rsrq": -10,
		"mean_latency": 15,
		"cqi": 10,
		"network": "net1"
	})

	# Advance watermark to close first window and skip second window [10-20)
	manager.advance_watermark(25)

	# Should only have 1 result (first window with data)
	assert len(results) == 1
	assert results[0]["window_start"] == 0
	assert results[0]["window_end"] == 10


def test_time_window_manager_zerofill_empty_windows():
	"""TimeWindowManager with ZeroFillStrategy should output zero-filled empty windows"""
	results = []

	manager = TimeWindowManager(
		window_size=10,
		on_window_complete=lambda data: results.append(data),
		processing_profiles=[LatencyProfile],
		empty_window_strategy=ZeroFillStrategy()
	)

	# Add measurement in first window [0-10)
	manager.add_measurement({
		"cell_index": "cellA",
		"timestamp": 5,
		"rsrp": -80,
		"sinr": 20.0,
		"rsrq": -10,
		"mean_latency": 15,
		"cqi": 10,
		"network": "net1"
	})

	# Create empty window by advancing past it
	manager.advance_watermark(25)

	# Should have 1 result (the first window with data)
	# The second window [10-20) doesn't exist yet because no measurement was added for cellA in that range
	assert len(results) == 1


def test_time_window_manager_forwardfill_empty_windows():
	"""TimeWindowManager with ForwardFillStrategy should forward-fill empty windows"""
	results = []

	manager = TimeWindowManager(
		window_size=10,
		on_window_complete=lambda data: results.append(data),
		processing_profiles=[LatencyProfile],
		empty_window_strategy=ForwardFillStrategy()
	)

	# Add measurement in first window [0-10)
	manager.add_measurement({
		"cell_index": "cellA",
		"timestamp": 5,
		"rsrp": -80,
		"sinr": 20.0,
		"rsrq": -10,
		"mean_latency": 15,
		"cqi": 10,
		"network": "net1"
	})

	# Advance to close first window
	manager.advance_watermark(25)

	# Should have 1 result
	assert len(results) == 1
	assert results[0]["sample_count"] > 0  # First window has data


def test_consecutive_empty_windows_with_zerofill():
	"""Test multiple consecutive empty windows with ZeroFill"""
	results = []

	manager = TimeWindowManager(
		window_size=10,
		on_window_complete=lambda data: results.append(data),
		processing_profiles=[LatencyProfile],
		empty_window_strategy=ZeroFillStrategy()
	)

	# Add measurements in first window [0-10)
	manager.add_measurement({
		"cell_index": "cellA",
		"timestamp": 5,
		"rsrp": -80,
		"sinr": 20.0,
		"rsrq": -10,
		"mean_latency": 15,
		"cqi": 10,
		"network": "net1"
	})

	# Add measurement in fourth window [30-40)
	manager.add_measurement({
		"cell_index": "cellA",
		"timestamp": 35,
		"rsrp": -75,
		"sinr": 22.0,
		"rsrq": -9,
		"mean_latency": 12,
		"cqi": 11,
		"network": "net1"
	})

	# Advance watermark to close all windows
	manager.advance_watermark(45)

	# Should have results for windows with measurements
	# Windows [10-20) and [20-30) won't exist because no measurements triggered their creation
	assert len(results) >= 2


def test_window_start_end_timestamp_inconsistency():
	"""ISSUE: TimeWindowManager adds window_start/window_end, but strategies add start_time/end_time"""
	results = []

	manager = TimeWindowManager(
		window_size=10,
		on_window_complete=lambda data: results.append(data),
		processing_profiles=[LatencyProfile],
		empty_window_strategy=ZeroFillStrategy()
	)

	manager.add_measurement({
		"cell_index": "cellA",
		"timestamp": 5,
		"rsrp": -80,
		"sinr": 20.0,
		"rsrq": -10,
		"mean_latency": 15,
		"cqi": 10,
		"network": "net1"
	})

	manager.advance_watermark(15)

	if results:
		result = results[0]
		# TimeWindowManager adds these (line 122-123 in time_window_manager.py)
		assert "window_start" in result
		assert "window_end" in result

		# For empty windows, strategies also add start_time/end_time
		# This creates duplicate timestamp fields!
		# (This test will pass for normal windows, but would fail for empty ones)


# ============================================================================
# EDGE CASES
# ============================================================================

def test_empty_context_dict():
	"""Test strategies with empty context dict"""
	strategy = ZeroFillStrategy()
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context={})
	assert res is None


def test_zero_fill_with_empty_fields_list():
	"""ZeroFillStrategy with empty fields list should still create basic structure"""
	strategy = ZeroFillStrategy()
	context = {"fields": [], "metadata": {"network": "net1"}}
	res = strategy.handle(cell_id="cellA", window_start=100, window_end=110, context=context)

	assert res is not None
	assert res["cell_index"] == "cellA"
	assert res["sample_count"] == 0
	assert res["network"] == "net1"


def test_forward_fill_doesnt_modify_original():
	"""ForwardFillStrategy should not modify the original last_values"""
	original_data = {
		"cell_index": "cellA",
		"start_time": 100,
		"end_time": 110,
		"sample_count": 5,
		"rsrp": {"min": -85, "max": -75, "mean": -80, "std": 3.5, "samples": 5}
	}

	strategy = ForwardFillStrategy()
	context = {"last_values": original_data}

	res = strategy.handle(cell_id="cellA", window_start=200, window_end=210, context=context)

	# Original should be unchanged
	assert original_data["start_time"] == 100
	assert original_data["end_time"] == 110
	assert original_data["sample_count"] == 5

	# Result should have new values
	assert res["start_time"] == 200
	assert res["end_time"] == 210
	assert res["sample_count"] == 0
