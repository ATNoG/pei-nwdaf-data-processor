import pytest
from src.window_buffer import CellWindowBuffer


class TestCellWindowBuffer:

    def test_empty_buffer(self):
        buf = CellWindowBuffer()
        assert len(buf) == 0
        assert buf.get_all() == []

    def test_append_slice(self):
        buf = CellWindowBuffer()
        records = [
            {"timestamp": 10, "value": 1},
            {"timestamp": 20, "value": 2},
        ]
        buf.append_slice(records)
        assert len(buf) == 2
        assert buf.get_all() == records

    def test_append_multiple_slices(self):
        buf = CellWindowBuffer()
        buf.append_slice([{"timestamp": 10, "value": 1}])
        buf.append_slice([{"timestamp": 20, "value": 2}, {"timestamp": 30, "value": 3}])
        assert len(buf) == 3
        result = buf.get_all()
        assert [r["timestamp"] for r in result] == [10, 20, 30]

    def test_evict_partial(self):
        buf = CellWindowBuffer()
        buf.append_slice([
            {"timestamp": 10, "value": 1},
            {"timestamp": 20, "value": 2},
            {"timestamp": 30, "value": 3},
            {"timestamp": 40, "value": 4},
        ])
        buf.evict_before(25)
        assert len(buf) == 2
        result = buf.get_all()
        assert [r["timestamp"] for r in result] == [30, 40]

    def test_evict_all(self):
        buf = CellWindowBuffer()
        buf.append_slice([
            {"timestamp": 10, "value": 1},
            {"timestamp": 20, "value": 2},
        ])
        buf.evict_before(100)
        assert len(buf) == 0
        assert buf.get_all() == []

    def test_evict_none(self):
        buf = CellWindowBuffer()
        buf.append_slice([
            {"timestamp": 10, "value": 1},
            {"timestamp": 20, "value": 2},
        ])
        buf.evict_before(5)
        assert len(buf) == 2

    def test_evict_exact_boundary(self):
        """Evict with cutoff equal to a record's timestamp — that record should NOT be evicted."""
        buf = CellWindowBuffer()
        buf.append_slice([
            {"timestamp": 10, "value": 1},
            {"timestamp": 20, "value": 2},
        ])
        buf.evict_before(10)
        assert len(buf) == 2
        # cutoff is < strict, so timestamp == 10 stays
        buf.evict_before(11)
        assert len(buf) == 1
        assert buf.get_all()[0]["timestamp"] == 20

    def test_clear(self):
        buf = CellWindowBuffer()
        buf.append_slice([{"timestamp": 10, "value": 1}])
        assert len(buf) == 1
        buf.clear()
        assert len(buf) == 0
        assert buf.get_all() == []

    def test_custom_time_field(self):
        buf = CellWindowBuffer(time_field="ts")
        buf.append_slice([
            {"ts": 10, "value": 1},
            {"ts": 20, "value": 2},
            {"ts": 30, "value": 3},
        ])
        buf.evict_before(15)
        assert len(buf) == 2
        assert buf.get_all()[0]["ts"] == 20

    def test_missing_time_field_evicted(self):
        """Records without the time field default to 0, so they get evicted by any positive cutoff."""
        buf = CellWindowBuffer()
        buf.append_slice([
            {"value": 1},  # no timestamp
            {"timestamp": 20, "value": 2},
        ])
        buf.evict_before(1)
        assert len(buf) == 1
        assert buf.get_all()[0]["timestamp"] == 20

    def test_get_all_returns_copy(self):
        """get_all returns a new list, not the internal deque."""
        buf = CellWindowBuffer()
        buf.append_slice([{"timestamp": 10, "value": 1}])
        result = buf.get_all()
        result.append({"timestamp": 99, "value": 99})
        assert len(buf) == 1  # internal data unchanged
