# tests/profiles/test_latency_profile.py
import pytest
from src.profiles.latency_profile import LatencyProfile

# Sample data fixtures
@pytest.fixture
def single_cell_data():
    return [
        {"cell_index": 123, "rsrp": -85, "sinr": 18.5, "rsrq": -12, "mean_latency": 25, "cqi": 12},
        {"cell_index": 123, "rsrp": -90, "sinr": 15.2, "rsrq": -14, "mean_latency": 30, "cqi": 10},
        {"cell_index": 123, "rsrp": -80, "sinr": 22.1, "rsrq": -10, "mean_latency": 20, "cqi": 14},
    ]

@pytest.fixture
def mixed_cell_data():
    return [
        {"cell_index": 123, "rsrp": -85, "mean_latency": 25},
        {"cell_index": 456, "rsrp": -88, "mean_latency": 22},
    ]

@pytest.fixture
def missing_cell_index_data():
    return [
        {"rsrp": -85, "mean_latency": 25},
        {"cell_index": 123, "rsrp": -90, "mean_latency": 30},
    ]

@pytest.fixture
def empty_data():
    return []

@pytest.fixture
def partial_fields_data():
    return [
        {"cell_index": 999, "rsrp": -70, "mean_latency": 18, "cqi": None},
        {"cell_index": 999, "rsrp": -75, "mean_latency": 22},
        {"cell_index": 999, "sinr": 20.0, "rsrq": -11},
    ]


def test_happy_path_same_cell(single_cell_data):
    result = LatencyProfile.process(single_cell_data)
    assert result is not None
    assert result["cell_index"] == 123
    assert result["num_samples"] == 3

    stats = result["stats"]
    assert stats["mean_latency"]["min"] == 20
    assert stats["mean_latency"]["max"] == 30
    assert stats["mean_latency"]["mean"] == 25.0
    assert stats["mean_latency"]["samples"] == 3
    assert stats["mean_latency"]["std"] > 0

    assert stats["rsrp"]["mean"] == -85.0
    assert stats["cqi"]["samples"] == 3


def test_mixed_cell_indices_returns_none(mixed_cell_data):
    result = LatencyProfile.process(mixed_cell_data)
    assert result is None


def test_missing_cell_index_in_any_entry_returns_none(missing_cell_index_data):
    result = LatencyProfile.process(missing_cell_index_data)
    assert result is None


def test_first_entry_missing_cell_index_returns_none():
    data = [{"rsrp": -80}, {"cell_index": 123, "mean_latency": 20}]
    assert LatencyProfile.process(data) is None


def test_empty_list_returns_none(empty_data):
    assert LatencyProfile.process(empty_data) is None


def test_handles_missing_or_non_numeric_fields_gracefully(partial_fields_data):
    result = LatencyProfile.process(partial_fields_data)
    assert result is not None
    assert result["cell_index"] == 999
    assert result["num_samples"] == 3

    stats = result["stats"]
    assert stats["rsrp"]["samples"] == 2
    assert stats["mean_latency"]["samples"] == 2
    assert stats["cqi"]["samples"] == 0
    assert stats["cqi"]["min"] is None
    assert stats["sinr"]["samples"] == 1
    assert stats["rsrq"]["samples"] == 1


def test_single_sample_std_is_zero():
    data = [{"cell_index": 1, "mean_latency": 42, "rsrp": -90}]
    result = LatencyProfile.process(data)
    assert result["stats"]["mean_latency"]["std"] == 0.0
    assert result["stats"]["mean_latency"]["samples"] == 1


def test_all_fields_none_or_missing():
    data = [
        {"cell_index": 5},
        {"cell_index": 5, "mean_latency": None},
    ]
    result = LatencyProfile.process(data)
    assert result is not None
    for field in LatencyProfile.FIELDS:
        assert result["stats"][field]["samples"] == 0
        assert result["stats"][field]["mean"] is None
