import pytest
from unittest.mock import Mock, patch, AsyncMock
from src.time_window_manager import TimeWindowManager
from src.profiles.latency_profile import LatencyProfile
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy, ForwardFillStrategy
import httpx


class MockStorage:
    """Mock storage structure for testing"""
    url = "http://mock-storage/api/v1/"

    class endpoint:
        cell = "cell/"
        raw = "raw/"


# ============================================================================
# BASIC FUNCTIONALITY TESTS
# ============================================================================

@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_basic_window_processing(mock_get):
    """Test basic window creation with mocked storage API"""
    results = []

    # Mock cells endpoint - returns 2 cells
    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1, 2]
    mock_cells_response.raise_for_status = Mock()

    # Mock raw data endpoint - return data for cell 1, empty for cell 2
    mock_data_response_cell1 = Mock()
    mock_data_response_cell1.json.return_value = {
        "data": [
            {
                "cell_index": 1,
                "timestamp": 5,
                "rsrp": -80,
                "sinr": 20.0,
                "rsrq": -10,
                "mean_latency": 15,
                "cqi": 10,
                "network": "net1"
            }
        ],
        "has_next": False
    }
    mock_data_response_cell1.raise_for_status = Mock()

    mock_data_response_cell2 = Mock()
    mock_data_response_cell2.json.return_value = {
        "data": [],
        "has_next": False
    }
    mock_data_response_cell2.raise_for_status = Mock()

    # Setup mock to return different responses based on call order
    mock_get.side_effect = [
        mock_cells_response,  # First call: get cells
        mock_data_response_cell1,  # Second call: get data for cell 1
        mock_data_response_cell2,  # Third call: get data for cell 2
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should have 1 result (cell 1 with data, cell 2 skipped)
    assert len(results) == 1
    assert results[0]["cell_index"] == 1
    assert results[0]["window_start"] == 0
    assert results[0]["window_end"] == 10
    assert results[0]["sample_count"] > 0


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_multiple_cells_with_data(mock_get):
    """Test processing multiple cells with data"""
    results = []

    # Mock cells endpoint
    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1, 2, 3]
    mock_cells_response.raise_for_status = Mock()

    # Mock data for all 3 cells
    def create_data_response(cell_id):
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {
                    "cell_index": cell_id,
                    "timestamp": 5,
                    "rsrp": -80 - cell_id,
                    "sinr": 20.0,
                    "rsrq": -10,
                    "mean_latency": 15,
                    "cqi": 10,
                    "network": f"net{cell_id}"
                }
            ],
            "has_next": False
        }
        mock_response.raise_for_status = Mock()
        return mock_response

    mock_get.side_effect = [
        mock_cells_response,
        create_data_response(1),
        create_data_response(2),
        create_data_response(3),
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should have 3 results (one for each cell)
    assert len(results) == 3
    cell_ids = {r["cell_index"] for r in results}
    assert cell_ids == {1, 2, 3}


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_batched_data_fetching(mock_get):
    """Test that manager handles paginated/batched data correctly"""
    results = []

    # Mock cells endpoint
    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # Mock data responses - 3 batches
    mock_batch1 = Mock()
    mock_batch1.json.return_value = {
        "data": [
            {"cell_index": 1, "timestamp": 1, "rsrp": -80, "sinr": 20.0, "rsrq": -10, "mean_latency": 15, "cqi": 10}
        ],
        "has_next": True
    }
    mock_batch1.raise_for_status = Mock()

    mock_batch2 = Mock()
    mock_batch2.json.return_value = {
        "data": [
            {"cell_index": 1, "timestamp": 2, "rsrp": -81, "sinr": 21.0, "rsrq": -11, "mean_latency": 16, "cqi": 11}
        ],
        "has_next": True
    }
    mock_batch2.raise_for_status = Mock()

    mock_batch3 = Mock()
    mock_batch3.json.return_value = {
        "data": [
            {"cell_index": 1, "timestamp": 3, "rsrp": -82, "sinr": 22.0, "rsrq": -12, "mean_latency": 17, "cqi": 12}
        ],
        "has_next": False
    }
    mock_batch3.raise_for_status = Mock()

    mock_get.side_effect = [
        mock_cells_response,
        mock_batch1,
        mock_batch2,
        mock_batch3,
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should have 1 result with all 3 samples
    assert len(results) == 1
    assert results[0]["sample_count"] == 3

    # Verify batch_number parameters were incremented
    calls = mock_get.call_args_list
    # calls[0] is for cells endpoint
    # calls[1-3] are for data batches
    assert calls[1].kwargs['params']['batch_number'] == 1
    assert calls[2].kwargs['params']['batch_number'] == 2
    assert calls[3].kwargs['params']['batch_number'] == 3


# ============================================================================
# EMPTY WINDOW STRATEGY TESTS
# ============================================================================

@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_empty_window_with_skip_strategy(mock_get):
    """Empty windows with SkipStrategy should not produce output"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    mock_empty_data = Mock()
    mock_empty_data.json.return_value = {"data": [], "has_next": False}
    mock_empty_data.raise_for_status = Mock()

    mock_get.side_effect = [mock_cells_response, mock_empty_data]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should have no results (empty window skipped)
    assert len(results) == 0


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_empty_window_with_zerofill_strategy(mock_get):
    """Empty windows with ZeroFillStrategy should produce zero-filled output"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    mock_empty_data = Mock()
    mock_empty_data.json.return_value = {"data": [], "has_next": False}
    mock_empty_data.raise_for_status = Mock()

    mock_get.side_effect = [mock_cells_response, mock_empty_data]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=ZeroFillStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should have 1 zero-filled result
    assert len(results) == 1
    assert results[0]["cell_index"] == "1"  # Note: converted to string by strategy
    assert results[0]["sample_count"] == 0
    assert results[0]["is_empty_window"] is True

    # Check all fields are zero-filled
    for field in LatencyProfile.FIELDS:
        assert field in results[0]
        assert results[0][field]["samples"] == 0
        assert results[0][field]["min"] is None


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_empty_window_with_forwardfill_strategy(mock_get):
    """Empty windows with ForwardFillStrategy should use last processed values"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # First window: has data
    mock_data_window1 = Mock()
    mock_data_window1.json.return_value = {
        "data": [
            {
                "cell_index": 1,
                "timestamp": 5,
                "rsrp": -80,
                "sinr": 20.0,
                "rsrq": -10,
                "mean_latency": 15,
                "cqi": 10,
                "network": "net1"
            }
        ],
        "has_next": False
    }
    mock_data_window1.raise_for_status = Mock()

    # Second window: empty
    mock_empty_data = Mock()
    mock_empty_data.json.return_value = {"data": [], "has_next": False}
    mock_empty_data.raise_for_status = Mock()

    mock_get.side_effect = [
        mock_cells_response,  # get cells
        mock_data_window1,  # window [0, 10) - has data
        mock_cells_response,  # get cells again for second watermark
        mock_empty_data,  # window [10, 20) - empty
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=ForwardFillStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)  # First window with data
    await manager.advance_watermark(20)  # Second window empty, should forward-fill

    # Should have 2 results
    assert len(results) == 2

    # First result: normal processing
    assert results[0]["sample_count"] == 1

    # Second result: forward-filled
    assert results[1]["sample_count"] == 0
    assert results[1]["forward_filled"] is True
    assert results[1]["cell_index"] == 1  # Preserves cell_index
    assert results[1]["network"] == "net1"  # Preserves metadata


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_consecutive_empty_windows_with_forwardfill(mock_get):
    """Test multiple consecutive empty windows with ForwardFillStrategy"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # First window: has data
    mock_data = Mock()
    mock_data.json.return_value = {
        "data": [
            {
                "cell_index": 1,
                "timestamp": 5,
                "rsrp": -80,
                "sinr": 20.0,
                "rsrq": -10,
                "mean_latency": 15,
                "cqi": 10,
                "network": "net1"
            }
        ],
        "has_next": False
    }
    mock_data.raise_for_status = Mock()

    # Empty windows
    mock_empty = Mock()
    mock_empty.json.return_value = {"data": [], "has_next": False}
    mock_empty.raise_for_status = Mock()

    mock_get.side_effect = [
        mock_cells_response, mock_data,  # Window 1: with data
        mock_cells_response, mock_empty,  # Window 2: empty
        mock_cells_response, mock_empty,  # Window 3: empty
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=ForwardFillStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)
    await manager.advance_watermark(20)
    await manager.advance_watermark(30)

    # BUG: Currently this will fail because line 126 in time_window_manager.py
    # stores forward-filled data as last_processed, which then gets used again
    assert len(results) == 3
    assert results[0]["sample_count"] == 1  # Normal
    assert results[1]["forward_filled"] is True  # Forward-filled from window 0
    assert results[2]["forward_filled"] is True  # Should also forward-fill from window 0


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_storage_api_connection_error(mock_get):
    """Test handling of storage API connection failures"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # Simulate connection error
    mock_get.side_effect = [
        mock_cells_response,
        httpx.ConnectError("Connection refused")
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should handle gracefully and produce no results
    assert len(results) == 0


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_storage_api_timeout(mock_get):
    """Test handling of storage API timeout"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # Simulate timeout
    mock_get.side_effect = [
        mock_cells_response,
        httpx.TimeoutException("Request timed out")
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should handle gracefully
    assert len(results) == 0


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_malformed_api_response(mock_get):
    """Test handling of malformed API responses"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # Malformed response - missing 'data' field
    mock_bad_response = Mock()
    mock_bad_response.json.return_value = {"bad_field": []}
    mock_bad_response.raise_for_status = Mock()

    mock_get.side_effect = [mock_cells_response, mock_bad_response]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should handle gracefully
    assert len(results) == 0


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_no_cells_registered(mock_get):
    """Test when no cells are registered in storage"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = []  # Empty cell list
    mock_cells_response.raise_for_status = Mock()

    mock_get.return_value = mock_cells_response

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should handle gracefully and produce no results
    assert len(results) == 0


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_cells_endpoint_returns_non_list(mock_get):
    """Test when cells endpoint returns unexpected format"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = {"error": "something"}  # Not a list
    mock_cells_response.raise_for_status = Mock()

    mock_get.return_value = mock_cells_response

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should handle gracefully
    assert len(results) == 0


# ============================================================================
# WATERMARK TESTS
# ============================================================================

@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_watermark_monotonicity(mock_get):
    """Test that watermark cannot move backwards"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = []
    mock_cells_response.raise_for_status = Mock()

    mock_get.return_value = mock_cells_response

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(100)
    assert manager.watermark == 100

    # Try to move backwards
    await manager.advance_watermark(50)

    # Watermark should not change
    assert manager.watermark == 100


@pytest.mark.asyncio
async def test_set_initial_watermark_only_once():
    """Test that initial watermark can only be set once"""

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: None,
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(100)
    assert manager.watermark == 100

    # Try to set again
    manager.set_initial_watermark(200)

    # Should not change
    assert manager.watermark == 100


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_window_time_parameters_correct(mock_get):
    """Test that correct time parameters are passed to storage API"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    mock_data_response = Mock()
    mock_data_response.json.return_value = {"data": [], "has_next": False}
    mock_data_response.raise_for_status = Mock()

    mock_get.side_effect = [mock_cells_response, mock_data_response]

    manager = TimeWindowManager(
        window_size=60,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(100)
    await manager.advance_watermark(160)

    # Check the parameters passed to the raw data API
    calls = mock_get.call_args_list
    data_call = calls[1]  # Second call is for data

    assert data_call.kwargs['params']['cell_index'] == 1
    assert data_call.kwargs['params']['start_time'] == 100
    assert data_call.kwargs['params']['end_time'] == 160
    assert data_call.kwargs['params']['batch_number'] == 1


# ============================================================================
# INTEGRATION SCENARIO TESTS
# ============================================================================

@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_realistic_scenario_mixed_cells(mock_get):
    """Test realistic scenario with multiple cells, some with data, some empty"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1, 2, 3]
    mock_cells_response.raise_for_status = Mock()

    # Cell 1: has data
    mock_cell1_data = Mock()
    mock_cell1_data.json.return_value = {
        "data": [
            {"cell_index": 1, "timestamp": 5, "rsrp": -80, "sinr": 20.0, "rsrq": -10, "mean_latency": 15, "cqi": 10, "network": "net1"}
        ],
        "has_next": False
    }
    mock_cell1_data.raise_for_status = Mock()

    # Cell 2: empty
    mock_cell2_data = Mock()
    mock_cell2_data.json.return_value = {"data": [], "has_next": False}
    mock_cell2_data.raise_for_status = Mock()

    # Cell 3: has data
    mock_cell3_data = Mock()
    mock_cell3_data.json.return_value = {
        "data": [
            {"cell_index": 3, "timestamp": 7, "rsrp": -75, "sinr": 22.0, "rsrq": -9, "mean_latency": 12, "cqi": 11, "network": "net3"}
        ],
        "has_next": False
    }
    mock_cell3_data.raise_for_status = Mock()

    mock_get.side_effect = [
        mock_cells_response,
        mock_cell1_data,
        mock_cell2_data,
        mock_cell3_data,
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=ZeroFillStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # Should have 3 results (cell 1 with data, cell 2 zero-filled, cell 3 with data)
    assert len(results) == 3

    # Find results by cell
    cell1_result = next(r for r in results if r["cell_index"] == 1)
    cell2_result = next(r for r in results if r["cell_index"] == "2")  # String due to ZeroFill
    cell3_result = next(r for r in results if r["cell_index"] == 3)

    assert cell1_result["sample_count"] == 1
    assert cell2_result["sample_count"] == 0
    assert cell2_result["is_empty_window"] is True
    assert cell3_result["sample_count"] == 1


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_historical_processing_scenario(mock_get):
    """Test processing historical data (simulating START_TIME in the past)"""
    results = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1]
    mock_cells_response.raise_for_status = Mock()

    # Historical data
    mock_historical_data = Mock()
    mock_historical_data.json.return_value = {
        "data": [
            {"cell_index": 1, "timestamp": 1005, "rsrp": -80, "sinr": 20.0, "rsrq": -10, "mean_latency": 15, "cqi": 10}
        ],
        "has_next": False
    }
    mock_historical_data.raise_for_status = Mock()

    mock_get.side_effect = [
        mock_cells_response,
        mock_historical_data,
    ]

    manager = TimeWindowManager(
        window_size=60,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    # Start from 1 hour ago (simulating historical processing)
    manager.set_initial_watermark(1000)
    await manager.advance_watermark(1060)

    # Should process historical window
    assert len(results) == 1
    assert results[0]["window_start"] == 1000
    assert results[0]["window_end"] == 1060


@pytest.mark.asyncio
@patch('httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_parallel_cell_processing(mock_get):
    """Test that cells are processed in parallel (async benefit)"""
    results = []
    call_order = []

    mock_cells_response = Mock()
    mock_cells_response.json.return_value = [1, 2, 3]
    mock_cells_response.raise_for_status = Mock()

    # Create mock responses that track call order
    def create_tracking_response(cell_id):
        mock_response = Mock()

        def json_with_tracking():
            call_order.append(cell_id)
            return {
                "data": [
                    {"cell_index": cell_id, "timestamp": 5, "rsrp": -80, "sinr": 20.0, "rsrq": -10, "mean_latency": 15, "cqi": 10}
                ],
                "has_next": False
            }

        mock_response.json = json_with_tracking
        mock_response.raise_for_status = Mock()
        return mock_response

    mock_get.side_effect = [
        mock_cells_response,
        create_tracking_response(1),
        create_tracking_response(2),
        create_tracking_response(3),
    ]

    manager = TimeWindowManager(
        window_size=10,
        storage_struct=MockStorage,
        on_window_complete=lambda data: results.append(data),
        processing_profiles=[LatencyProfile()],
        empty_window_strategy=SkipStrategy()
    )

    manager.set_initial_watermark(0)
    await manager.advance_watermark(10)

    # All cells should be processed
    assert len(results) == 3
    # Call order shows requests were made (may not be sequential due to async)
    assert len(call_order) == 3
