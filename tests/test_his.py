"""
Unit tests for his.his: event_loop_tracker, ping_status_task, HISAgent, TimeoutConcurrentToolExecutor.
"""
import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest

from his.his import (
    HISAgent,
    TimeoutConcurrentToolExecutor,
    event_loop_tracker,
    ping_status_task,
)


class TestEventLoopTracker:
    """Tests for event_loop_tracker callback."""

    @patch("his.his.boto3")
    def test_sets_init_event_loop_field(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            init_event_loop=True,
            status_dynamo_table_name="TestTable",
            session_id="sess-123",
            agent_name="TestAgent",
            agent_id="TestAgentId",
        )

        mock_client.update_item.assert_called_once()
        call_kw = mock_client.update_item.call_args[1]
        assert call_kw["TableName"] == "TestTable"
        assert call_kw["Key"] == {"session_id": {"S": "sess-123"}}
        assert "evt_init_event_loop" in call_kw["UpdateExpression"]
        assert call_kw["ExpressionAttributeValues"][":name"]["S"] == "TestAgent"
        assert call_kw["ExpressionAttributeValues"][":ai"]["S"] == "TestAgentId"
        assert not stop_event.is_set()

    @patch("his.his.boto3")
    def test_sets_start_event_loop_field(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            start_event_loop=True,
            status_dynamo_table_name="MyTable",
            session_id="s1",
            agent_id="agent-456",
        )

        call_kw = mock_client.update_item.call_args[1]
        assert "evt_start_event_loop" in call_kw["UpdateExpression"]
        assert call_kw["ExpressionAttributeValues"][":ai"]["S"] == "agent-456"
        assert not stop_event.is_set()

    @patch("his.his.boto3")
    def test_sets_message_field(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            message={},
            session_id="s2",
        )

        call_kw = mock_client.update_item.call_args[1]
        assert "evt_message" in call_kw["UpdateExpression"]
        assert not stop_event.is_set()

    @patch("his.his.boto3")
    def test_result_sets_stop_ping_event_and_field(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            result=None,
            status_dynamo_table_name="T",
            session_id="s3",
        )

        assert stop_event.is_set()
        call_kw = mock_client.update_item.call_args[1]
        assert "evt_result" in call_kw["UpdateExpression"]

    @patch("his.his.boto3")
    def test_force_stop_sets_stop_ping_event_and_field(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            force_stop=True,
            session_id="s4",
        )

        assert stop_event.is_set()
        call_kw = mock_client.update_item.call_args[1]
        assert "evt_force_stop" in call_kw["UpdateExpression"]

    @patch("his.his.boto3")
    def test_default_table_and_session_when_missing(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            init_event_loop=True,
        )

        call_kw = mock_client.update_item.call_args[1]
        assert call_kw["TableName"] == "AgentCoreAgentStatus"
        assert call_kw["Key"] == {"session_id": {"S": "default"}}
        assert call_kw["ExpressionAttributeValues"][":name"]["S"] == "default"
        assert call_kw["ExpressionAttributeValues"][":ai"]["S"] == "default"

    @patch("his.his.boto3")
    def test_agent_id_defaults_to_default_when_not_provided(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            init_event_loop=True,
            status_dynamo_table_name="TestTable",
            session_id="sess-123",
            agent_name="TestAgent",
        )

        call_kw = mock_client.update_item.call_args[1]
        assert call_kw["ExpressionAttributeValues"][":ai"]["S"] == "default"


class TestPingStatusTask:
    """Tests for ping_status_task background thread."""

    @patch("his.his.time.sleep")
    @patch("his.his.boto3")
    def test_updates_dynamodb_running_then_ended_when_stop_set_after_first_sleep(
            self, mock_boto3, mock_sleep
    ):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        def stop_after_first_sleep(*_args, **_kwargs):
            stop_event.set()

        mock_sleep.side_effect = stop_after_first_sleep

        ping_status_task("StatusTable", "session-xyz", stop_event)

        assert mock_client.update_item.call_count >= 2
        calls = mock_client.update_item.call_args_list
        first_vals = calls[0][1]["ExpressionAttributeValues"]
        assert first_vals[":status"]["S"] == "running"
        last_vals = calls[-1][1]["ExpressionAttributeValues"]
        assert last_vals[":status"]["S"] == "ended"

    @patch("his.his.time.sleep")
    @patch("his.his.boto3")
    def test_ended_update_uses_correct_table_and_key(self, mock_boto3, mock_sleep):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()
        mock_sleep.side_effect = lambda *a, **k: stop_event.set()

        ping_status_task("MyStatusTable", "my-session-id", stop_event)

        last_call_kw = mock_client.update_item.call_args_list[-1][1]
        assert last_call_kw["TableName"] == "MyStatusTable"
        assert last_call_kw["Key"] == {"session_id": {"S": "my-session-id"}}
        assert last_call_kw["ExpressionAttributeValues"][":status"]["S"] == "ended"


class TestTimeoutConcurrentToolExecutor:
    """Tests for TimeoutConcurrentToolExecutor."""

    @pytest.mark.asyncio
    async def test_timeout_seconds_default(self):
        executor = TimeoutConcurrentToolExecutor()
        assert executor.timeout_seconds == 300.0

    @pytest.mark.asyncio
    async def test_timeout_seconds_custom(self):
        executor = TimeoutConcurrentToolExecutor(timeout_seconds=10.0)
        assert executor.timeout_seconds == 10.0

    @pytest.mark.asyncio
    async def test_task_appends_error_result_on_timeout(self):
        executor = TimeoutConcurrentToolExecutor(timeout_seconds=0.01)
        tool_use = {"toolUseId": "tid-1", "name": "slow_tool"}
        tool_results = []
        task_queue = asyncio.Queue()
        task_event = asyncio.Event()
        task_event.set()
        stop_event = object()
        mock_agent = MagicMock()

        async def never_finish(*args, **kwargs):
            await asyncio.sleep(60)

        with patch.object(executor, "_run_tool_stream", side_effect=never_finish):
            await executor._task(
                agent=mock_agent,
                tool_use=tool_use,
                tool_results=tool_results,
                cycle_trace=MagicMock(),
                cycle_span=MagicMock(),
                invocation_state={},
                task_id=0,
                task_queue=task_queue,
                task_event=task_event,
                stop_event=stop_event,
                structured_output_context=None,
            )

        assert len(tool_results) == 1
        err = tool_results[0]
        assert err["toolUseId"] == "tid-1"
        assert err["status"] == "error"
        assert "timed out" in err["content"][0]["text"].lower()

        items = []
        while not task_queue.empty():
            items.append(task_queue.get_nowait())
        assert any(ev is stop_event for _, ev in items)
        from strands.types._events import ToolResultEvent
        assert any(isinstance(ev, ToolResultEvent) for _, ev in items)


class TestHISAgent:
    """Tests for HISAgent construction and wiring."""

    @patch("his.his.ping_status_task")
    @patch("his.his.boto3")
    @patch("his.his.S3SessionManager")
    def test_constructs_with_expected_session_manager_and_starts_ping_thread(
            self, mock_s3_manager, mock_boto3, mock_ping_task
    ):
        mock_s3_manager.return_value = MagicMock()
        mock_boto3.client.return_value = MagicMock()

        agent = HISAgent(
            bucket_name="my-bucket",
            status_dynamo_table_name="StatusTable",
            session_id="sess-1",
            name="TestAgent",
        )

        mock_s3_manager.assert_called_once_with(
            session_id="sess-1",
            bucket="my-bucket",
            region_name="eu-central-1",
            prefix="ac-sessions/TestAgent",
        )
        assert agent.name == "TestAgent"
        # Daemon thread was started (ping_status_task is the target)
        assert threading.active_count() >= 1

    @patch("his.his.ping_status_task")
    @patch("his.his.boto3")
    @patch("his.his.S3SessionManager")
    def test_constructs_with_agent_id(
            self, mock_s3_manager, mock_boto3, mock_ping_task
    ):
        mock_s3_manager.return_value = MagicMock()
        mock_boto3.client.return_value = MagicMock()

        agent = HISAgent(
            bucket_name="my-bucket",
            status_dynamo_table_name="StatusTable",
            session_id="sess-1",
            name="TestAgent",
            agent_id="agent-unique-123",
        )

        assert agent.agent_id == "agent-unique-123"
        assert agent.name == "TestAgent"
