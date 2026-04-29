import asyncio
import pytest
from src.time_window_manager import TimeWindowManager
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy


def make_record(sst="1", sd="000001", dnn="internet", event="PERF_DATA", ts=5, **metrics):
    return {
        "tags": {"snssai_sst": sst, "snssai_sd": sd, "dnn": dnn},
        "event": event,
        "timestamp": ts,
        "metrics": metrics if metrics else {"thrputUl_mbps": 10.0, "thrputDl_mbps": 50.0},
    }


def make_wm(**kwargs):
    results = kwargs.pop("results", [])
    defaults = {
        "window_size": 10,
        "on_window_complete": lambda d: results.append(d),
        "empty_window_strategy": SkipStrategy(),
    }
    defaults.update(kwargs)
    return TimeWindowManager(**defaults), results


# ============================================================================
# BASIC INGESTION AND WINDOWING
# ============================================================================

async def test_basic_ingest_and_window():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=5, thrputUl_mbps=10.0))
    await wm.advance_watermark(10)

    assert len(results) == 1
    assert results[0]["sample_count"] == 1
    assert results[0]["tags"]["snssai_sst"] == "1"
    assert results[0]["tags"]["event"] == "PERF_DATA"
    assert "thrputUl_mbps" in results[0]["metrics"]


async def test_output_format():
    results = []
    wm, _ = make_wm(results=results, window_size=60)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=30, thrputUl_mbps=10.0))
    await wm.advance_watermark(60)

    r = results[0]
    assert set(r.keys()) >= {"tags", "window_start", "window_end", "window_duration_seconds", "sample_count", "metrics"}
    assert r["window_start"] == 0
    assert r["window_end"] == 60
    assert r["window_duration_seconds"] == 60


async def test_numeric_aggregation():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=1, thrputUl_mbps=10.0))
    wm.ingest(make_record(ts=2, thrputUl_mbps=20.0))
    wm.ingest(make_record(ts=3, thrputUl_mbps=30.0))
    await wm.advance_watermark(10)

    m = results[0]["metrics"]["thrputUl_mbps"]
    assert m["mean"] == 20.0
    assert m["min"] == 10.0
    assert m["max"] == 30.0
    assert m["count"] == 3
    assert m["std"] > 0


async def test_non_numeric_metrics_skipped():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest({
        "tags": {"snssai_sst": "1", "dnn": "internet"},
        "event": "UE_MOBILITY",
        "timestamp": 5,
        "metrics": {"trajectory": [{"ts": 1, "nrCellId": "abc"}], "speed": 30.5},
    })
    await wm.advance_watermark(10)

    assert len(results) == 1
    assert "speed" in results[0]["metrics"]
    assert "trajectory" not in results[0]["metrics"]


async def test_all_non_numeric_metrics_produces_empty_metrics_dict():
    """Records where every metric is non-numeric yield sample_count>0 but empty metrics."""
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest({
        "tags": {"snssai_sst": "1", "dnn": "internet"},
        "event": "UE_MOBILITY",
        "timestamp": 5,
        "metrics": {"trajectory": [{"ts": 1}], "label": "handover"},
    })
    await wm.advance_watermark(10)

    assert len(results) == 1
    assert results[0]["sample_count"] == 1
    assert results[0]["metrics"] == {}


async def test_multiple_metrics_all_aggregated():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=1, thrputUl_mbps=10.0, thrputDl_mbps=50.0, pdb_ms=20.0))
    wm.ingest(make_record(ts=2, thrputUl_mbps=20.0, thrputDl_mbps=60.0, pdb_ms=30.0))
    await wm.advance_watermark(10)

    metrics = results[0]["metrics"]
    assert set(metrics.keys()) == {"thrputUl_mbps", "thrputDl_mbps", "pdb_ms"}
    assert metrics["thrputDl_mbps"]["mean"] == 55.0


# ============================================================================
# GROUP KEY EXTRACTION
# ============================================================================

async def test_missing_sst_tag_skipped():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest({"tags": {"dnn": "internet"}, "event": "PERF_DATA", "timestamp": 5, "metrics": {"x": 1.0}})
    await wm.advance_watermark(10)
    assert len(results) == 0


async def test_missing_dnn_tag_skipped():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest({"tags": {"snssai_sst": "1"}, "event": "PERF_DATA", "timestamp": 5, "metrics": {"x": 1.0}})
    await wm.advance_watermark(10)
    assert len(results) == 0


async def test_different_events_separate_groups():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(event="PERF_DATA", ts=5, thrputUl_mbps=10.0))
    wm.ingest(make_record(event="UE_MOBILITY", ts=5, speed=50.0))
    await wm.advance_watermark(10)

    assert len(results) == 2
    events = {r["tags"]["event"] for r in results}
    assert events == {"PERF_DATA", "UE_MOBILITY"}


async def test_different_slices_separate_groups():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(sst="1", dnn="internet", ts=5, thrputUl_mbps=10.0))
    wm.ingest(make_record(sst="2", dnn="ims", ts=5, thrputUl_mbps=5.0))
    await wm.advance_watermark(10)

    assert len(results) == 2
    slices = {(r["tags"]["snssai_sst"], r["tags"]["dnn"]) for r in results}
    assert slices == {("1", "internet"), ("2", "ims")}


# ============================================================================
# EMPTY WINDOW STRATEGIES
# ============================================================================

async def test_empty_window_skip():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(0)
    await wm.advance_watermark(10)
    assert len(results) == 0


async def test_empty_window_zerofill_after_data():
    results = []
    wm, _ = make_wm(results=results, empty_window_strategy=ZeroFillStrategy())
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=5, thrputUl_mbps=10.0))
    await wm.advance_watermark(10)   # has data
    await wm.advance_watermark(20)   # empty → zerofill

    assert len(results) == 2
    assert results[1]["sample_count"] == 0
    assert results[1]["metrics"]["thrputUl_mbps"]["count"] == 0
    assert results[1]["metrics"]["thrputUl_mbps"]["mean"] is None


# ============================================================================
# SLIDING WINDOW
# ============================================================================

async def test_sliding_window_accumulates():
    results = []
    wm, _ = make_wm(results=results, window_size=30, slide_interval=10)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=5, thrputUl_mbps=100.0))
    await wm.advance_watermark(10)
    wm.ingest(make_record(ts=15, thrputUl_mbps=200.0))
    await wm.advance_watermark(20)
    wm.ingest(make_record(ts=25, thrputUl_mbps=300.0))
    await wm.advance_watermark(30)

    assert results[0]["sample_count"] == 1
    assert results[1]["sample_count"] == 2
    assert results[2]["sample_count"] == 3


async def test_sliding_window_eviction():
    results = []
    wm, _ = make_wm(results=results, window_size=20, slide_interval=10)
    wm.set_initial_watermark(0)
    wm.ingest(make_record(ts=5, thrputUl_mbps=100.0))
    await wm.advance_watermark(10)
    wm.ingest(make_record(ts=15, thrputUl_mbps=200.0))
    await wm.advance_watermark(20)
    wm.ingest(make_record(ts=25, thrputUl_mbps=300.0))
    await wm.advance_watermark(30)  # window [10,30): ts=5 evicted

    assert results[0]["sample_count"] == 1
    assert results[1]["sample_count"] == 2
    assert results[2]["sample_count"] == 2  # ts=5 evicted


async def test_slide_interval_validation():
    with pytest.raises(ValueError):
        TimeWindowManager(window_size=10, slide_interval=0)
    with pytest.raises(ValueError):
        TimeWindowManager(window_size=10, slide_interval=-5)
    with pytest.raises(ValueError):
        TimeWindowManager(window_size=10, slide_interval=20)


# ============================================================================
# WATERMARK
# ============================================================================

async def test_watermark_monotonicity():
    results = []
    wm, _ = make_wm(results=results)
    wm.set_initial_watermark(100)
    await wm.advance_watermark(50)
    assert wm.watermark == 100
    assert len(results) == 0  # nothing emitted on backwards watermark


async def test_set_initial_watermark_only_once():
    wm, _ = make_wm()
    wm.set_initial_watermark(100)
    wm.set_initial_watermark(200)
    assert wm.watermark == 100


# ============================================================================
# ISO TIMESTAMP NORMALIZATION
# ============================================================================

async def test_iso_timestamp_normalized_and_ingested():
    """ISO timestamp strings are converted to int unix epoch before buffering."""
    results = []
    wm, _ = make_wm(results=results, window_size=60)
    wm.set_initial_watermark(0)
    # 2020-01-01T00:00:05Z = unix 1577836805
    wm.ingest({
        "tags": {"snssai_sst": "1", "snssai_sd": "", "dnn": "internet"},
        "event": "PERF_DATA",
        "timestamp": "2020-01-01T00:00:05Z",
        "metrics": {"x": 1.0},
    })
    # The record's epoch is 1577836805, so set watermark past that
    wm.set_initial_watermark.__func__  # no-op, just verify it's set
    wm.watermark = 1577836800
    wm._can_change_watermark = False
    await wm.advance_watermark(1577836860)

    assert len(results) == 1
    assert results[0]["sample_count"] == 1


# ============================================================================
# TTL PRUNING
# ============================================================================

async def test_stale_group_pruned_after_ttl():
    """A group that stops sending data is pruned after TTL expires."""
    results = []
    wm, _ = make_wm(results=results, group_ttl=0.1)  # 100ms TTL
    wm.set_initial_watermark(0)

    key = ("1", "000001", "internet", "PERF_DATA")
    wm.ingest(make_record(ts=5))
    await wm.advance_watermark(10)
    assert key in wm._buffers

    # Wait for TTL to expire, then trigger watermark
    await asyncio.sleep(0.15)
    await wm.advance_watermark(20)

    assert key not in wm._buffers
    assert key not in wm._last_processed


async def test_active_group_not_pruned_within_ttl():
    """A group that keeps sending data is never pruned."""
    results = []
    wm, _ = make_wm(results=results, group_ttl=5)  # 5s TTL — won't expire in test
    wm.set_initial_watermark(0)

    key = ("1", "000001", "internet", "PERF_DATA")
    wm.ingest(make_record(ts=5))
    await wm.advance_watermark(10)
    wm.ingest(make_record(ts=15))
    await wm.advance_watermark(20)

    assert key in wm._buffers


async def test_last_window_emitted_before_pruning():
    """The final window of a dying group must be emitted before the group is pruned."""
    results = []
    wm, _ = make_wm(results=results, group_ttl=0.1)
    wm.set_initial_watermark(0)

    wm.ingest(make_record(ts=5, thrputUl_mbps=42.0))
    await wm.advance_watermark(10)
    assert len(results) == 1  # emitted

    await asyncio.sleep(0.15)
    await wm.advance_watermark(20)  # prune fires AFTER emission loop

    # Group is pruned, but the window at [0,10) was already emitted
    key = ("1", "000001", "internet", "PERF_DATA")
    assert key not in wm._buffers
    assert len(results) == 1  # no extra emission, just the original one


async def test_no_pruning_when_ttl_not_set():
    """Without group_ttl, buffers are never pruned regardless of inactivity."""
    results = []
    wm, _ = make_wm(results=results, group_ttl=None)
    wm.set_initial_watermark(0)

    wm.ingest(make_record(ts=5))
    await wm.advance_watermark(10)
    await wm.advance_watermark(20)
    await wm.advance_watermark(30)

    key = ("1", "000001", "internet", "PERF_DATA")
    assert key in wm._buffers
