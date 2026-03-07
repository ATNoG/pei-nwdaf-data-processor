# tests/profiles/test_metric_profile.py
import pytest
from src.profiles.metric_profile import MetricProfile


@pytest.fixture
def single_cell_data():
    return [
        {"cell_index": 123, "rsrp": -85, "sinr": 18.5, "rsrq": -12, "mean_latency": 25, "cqi": 12},
        {"cell_index": 123, "rsrp": -90, "sinr": 15.2, "rsrq": -14, "mean_latency": 30, "cqi": 10},
        {"cell_index": 123, "rsrp": -80, "sinr": 22.1, "rsrq": -10, "mean_latency": 20, "cqi": 14},
    ]


@pytest.fixture
def multi_cell_data():
    return [
        {"cell_index": 123, "rsrp": -85, "mean_latency": 25},
        {"cell_index": 456, "rsrp": -88, "mean_latency": 22},
    ]


@pytest.fixture
def data_with_src_ip():
    return [
        {"cell_index": 1, "src_ip": "10.0.0.1", "rsrp": -80, "mean_latency": 10},
        {"cell_index": 1, "src_ip": "10.0.0.1", "rsrp": -82, "mean_latency": 12},
        {"cell_index": 1, "src_ip": "10.0.0.2", "rsrp": -90, "mean_latency": 30},
    ]


@pytest.fixture
def data_mixed_src_ip():
    """Some records have src_ip, some don't — should fall back to cell_index grouping."""
    return [
        {"cell_index": 1, "src_ip": "10.0.0.1", "rsrp": -80},
        {"cell_index": 1, "rsrp": -85},
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
    results = MetricProfile.process(single_cell_data)
    assert len(results) == 1
    result = results[0]
    assert result["cell_index"] == 123
    assert result["sample_count"] == 3

    assert result["mean_latency"]["min"] == 20
    assert result["mean_latency"]["max"] == 30
    assert result["mean_latency"]["mean"] == 25.0
    assert result["mean_latency"]["samples"] == 3
    assert result["mean_latency"]["std"] > 0

    assert result["rsrp"]["mean"] == -85.0
    assert result["cqi"]["samples"] == 3


def test_multi_cell_returns_separate_groups(multi_cell_data):
    results = MetricProfile.process(multi_cell_data)
    assert len(results) == 2
    cell_ids = {r["cell_index"] for r in results}
    assert cell_ids == {123, 456}


def test_groups_by_cell_and_src_ip(data_with_src_ip):
    results = MetricProfile.process(data_with_src_ip)
    assert len(results) == 2

    by_ip = {r["src_ip"]: r for r in results}
    assert "10.0.0.1" in by_ip
    assert "10.0.0.2" in by_ip

    assert by_ip["10.0.0.1"]["sample_count"] == 2
    assert by_ip["10.0.0.2"]["sample_count"] == 1
    assert by_ip["10.0.0.1"]["rsrp"]["mean"] == -81.0
    assert by_ip["10.0.0.2"]["mean_latency"]["mean"] == 30.0


def test_fallback_to_cell_when_src_ip_missing(data_mixed_src_ip):
    results = MetricProfile.process(data_mixed_src_ip)
    assert len(results) == 1
    assert results[0]["cell_index"] == 1
    assert "src_ip" not in results[0]
    assert results[0]["sample_count"] == 2


def test_missing_cell_index_returns_empty():
    data = [{"rsrp": -80}, {"rsrp": -85}]
    results = MetricProfile.process(data)
    assert results == []


def test_empty_list_returns_empty(empty_data):
    assert MetricProfile.process(empty_data) == []


def test_handles_missing_or_non_numeric_fields(partial_fields_data):
    results = MetricProfile.process(partial_fields_data)
    assert len(results) == 1
    result = results[0]
    assert result["cell_index"] == 999
    assert result["sample_count"] == 3

    assert result["rsrp"]["samples"] == 2
    assert result["mean_latency"]["samples"] == 2
    assert result["sinr"]["samples"] == 1
    assert result["rsrq"]["samples"] == 1


def test_single_sample_std_is_zero():
    data = [{"cell_index": 1, "mean_latency": 42, "rsrp": -90}]
    results = MetricProfile.process(data)
    assert len(results) == 1
    assert results[0]["mean_latency"]["std"] == 0.0
    assert results[0]["mean_latency"]["samples"] == 1


def test_all_fields_none_or_missing():
    data = [
        {"cell_index": 5},
        {"cell_index": 5, "mean_latency": None},
    ]
    results = MetricProfile.process(data)
    assert len(results) == 1
    # No numeric fields found, so stats should be empty (no metric keys)
    result = results[0]
    assert result["sample_count"] == 2
    assert "mean_latency" not in result


def test_aggregates_all_numeric_fields_dynamically():
    """Any numeric field should be aggregated, not just hardcoded ones."""
    data = [
        {"cell_index": 1, "custom_metric": 10.0, "another_one": 5},
        {"cell_index": 1, "custom_metric": 20.0, "another_one": 15},
    ]
    results = MetricProfile.process(data)
    assert len(results) == 1
    assert results[0]["custom_metric"]["mean"] == 15.0
    assert results[0]["another_one"]["mean"] == 10.0


def test_metadata_carried_through():
    data = [
        {"cell_index": 1, "network": "5G", "server_ip": "8.8.8.8", "rsrp": -80},
        {"cell_index": 1, "network": "5G", "server_ip": "8.8.8.8", "rsrp": -85},
    ]
    results = MetricProfile.process(data)
    assert results[0]["network"] == "5G"
    assert results[0]["server_ip"] == "8.8.8.8"
