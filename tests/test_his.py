"""
Unit tests for his.his: event_loop_tracker, ping_status_task, HISAgent,
TimeoutConcurrentToolExecutor, and write_dynamo.
"""
import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest

from his.his import (
    DEFAULT_DYNAMODB_REPORTING_PROMPT_TEMPLATE,
    HISAgent,
    HISBedrockThrottlingLogger,
    TimeoutConcurrentToolExecutor,
    event_loop_tracker,
    ping_status_task,
    write_dynamo,
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

        mock_client.put_item.assert_called_once()
        call_kw = mock_client.put_item.call_args[1]
        assert call_kw["TableName"] == "TestTable"
        assert call_kw["Item"]["session_id"]["S"] == "sess-123"
        assert "event_id" in call_kw["Item"]
        assert call_kw["Item"]["agent_id"]["S"] == "TestAgentId"
        assert call_kw["Item"]["evt_type"]["S"] == "EVENT_LOOP"
        assert call_kw["Item"]["evt_message"]["S"] == "init_event_loop"
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
        call_kw = mock_client.put_item.call_args[1]
        assert call_kw["Item"]["evt_message"]["S"] == "result"

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
        call_kw = mock_client.put_item.call_args[1]
        assert call_kw["Item"]["evt_message"]["S"] == "force_stop"

    @patch("his.his.boto3")
    def test_default_table_and_session_when_missing(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        event_loop_tracker(
            stop_ping_event=stop_event,
            init_event_loop=True,
        )

        call_kw = mock_client.put_item.call_args[1]
        assert call_kw["TableName"] == "AgentCoreAgentStatus"
        assert call_kw["Item"]["session_id"]["S"] == "default"
        assert call_kw["Item"]["agent_id"]["S"] == "default"

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

        call_kw = mock_client.put_item.call_args[1]
        assert call_kw["Item"]["agent_id"]["S"] == "default"


class TestPingStatusTask:
    """Tests for ping_status_task background thread."""

    @patch("his.his.time.sleep")
    @patch("his.his.boto3")
    def test_updates_dynamodb_running_then_finished_when_stop_set_after_first_sleep(
            self, mock_boto3, mock_sleep
    ):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()

        def stop_after_first_sleep(*_args, **_kwargs):
            stop_event.set()

        mock_sleep.side_effect = stop_after_first_sleep

        ping_status_task("StatusTable", "session-xyz", "agent-123", stop_event)

        assert mock_client.put_item.call_count >= 2
        calls = mock_client.put_item.call_args_list
        first_item = calls[0][1]["Item"]
        assert first_item["evt_message"]["S"] == "running"
        assert first_item["agent_id"]["S"] == "agent-123"
        assert "event_id" in first_item
        last_item = calls[-1][1]["Item"]
        assert last_item["evt_message"]["S"] == "finished"
        assert "event_id" in last_item
        assert first_item["event_id"]["S"] != last_item["event_id"]["S"]

    @patch("his.his.time.sleep")
    @patch("his.his.boto3")
    def test_finished_update_uses_correct_table_and_key(self, mock_boto3, mock_sleep):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        stop_event = threading.Event()
        mock_sleep.side_effect = lambda *a, **k: stop_event.set()

        ping_status_task("MyStatusTable", "my-session-id", "my-agent-id", stop_event)

        last_call_kw = mock_client.put_item.call_args_list[-1][1]
        assert last_call_kw["TableName"] == "MyStatusTable"
        assert last_call_kw["Item"]["session_id"]["S"] == "my-session-id"
        assert last_call_kw["Item"]["agent_id"]["S"] == "my-agent-id"
        assert last_call_kw["Item"]["evt_message"]["S"] == "finished"
        assert "event_id" in last_call_kw["Item"]


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
            prefix="ac-sessions/TestAgent",
        )
        assert agent.name == "TestAgent"
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


class TestWriteDynamo:
    """Tests for write_dynamo tool."""

    @patch("his.his.boto3")
    def test_writes_event_to_dynamodb(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        write_dynamo(
            table_name="events-table",
            agent_id="agent-123",
            session_id="sess-456",
            event_type="STATUS",
            message="Processing started",
        )

        mock_client.put_item.assert_called_once()
        call_kw = mock_client.put_item.call_args[1]
        assert call_kw["TableName"] == "events-table"
        assert call_kw["Item"]["session_id"]["S"] == "sess-456"
        assert "event_id" in call_kw["Item"]
        assert call_kw["Item"]["agent_id"]["S"] == "agent-123"
        assert call_kw["Item"]["evt_type"]["S"] == "STATUS"
        assert call_kw["Item"]["evt_message"]["S"] == "Processing started"

    @patch("his.his.boto3")
    @patch("his.his.logger")
    def test_logs_error_on_dynamodb_failure(self, mock_logger, mock_boto3):
        mock_client = MagicMock()
        mock_client.put_item.side_effect = Exception("DynamoDB error")
        mock_boto3.client.return_value = mock_client

        write_dynamo(
            table_name="events-table",
            agent_id="agent-123",
            session_id="sess-456",
            event_type="STATUS",
            message="Test message",
        )

        mock_logger.error.assert_called_once()
        assert "sess-456" in mock_logger.error.call_args[0][0]


class TestLogResultAndExtractJson:
    """Tests for HISAgent._log_result and HISAgent._extract_json."""

    def test_extract_json_direct(self):
        text = '{"foo": "bar", "value": 1}'
        result = HISAgent._extract_json(text)
        assert result == {"foo": "bar", "value": 1}

    def test_extract_json_from_markdown_fence(self):
        text = "Here is the result:\n```json\n{\"foo\": \"bar\"}\n```"
        result = HISAgent._extract_json(text)
        assert result == {"foo": "bar"}

    def test_extract_json_from_embedded_object(self):
        text = "Some text before {\"foo\": \"bar\", \"n\": 2} and after."
        result = HISAgent._extract_json(text)
        assert result == {"foo": "bar", "n": 2}

    def test_extract_json_falls_back_to_raw(self):
        text = "this is not json"
        result = HISAgent._extract_json(text)
        assert result == {"raw": text}

class TestHISBedrockThrottlingLogger:
    """Tests for HISBedrockThrottlingLogger."""

    @pytest.mark.asyncio
    async def test_logs_bedrock_throttling_on_retry(self, monkeypatch):
        from strands.hooks.events import AfterModelCallEvent
        from strands.types.exceptions import ModelThrottledException

        # Arrange: create strategy and event with throttling exception
        strategy = HISBedrockThrottlingLogger(
            table_name="StatusTable",
            session_id="sess-1",
            agent_id="agent-123",
        )

        event = AfterModelCallEvent(
            agent=MagicMock(),
            invocation_state={"model_id": "bedrock-model", "operation": "invoke"},
            stop_response=None,
            exception=ModelThrottledException("throttled"),
            retry=False,
        )

        # Patch write_dynamo to capture calls
        calls: list[dict] = []

        def fake_write_dynamo(table_name, agent_id, session_id, event_type, message):
            calls.append(
                {
                    "table_name": table_name,
                    "agent_id": agent_id,
                    "session_id": session_id,
                    "event_type": event_type,
                    "message": message,
                }
            )

        monkeypatch.setattr("his.his.write_dynamo", fake_write_dynamo)

        # Act
        await strategy._handle_after_model_call(event)

        # Assert: DynamoDB throttling event was written
        assert len(calls) == 1
        call = calls[0]
        assert call["table_name"] == "StatusTable"
        assert call["session_id"] == "sess-1"
        assert call["agent_id"] == "agent-123"
        assert call["event_type"] == "BEDROCK_THROTTLING"
        assert "Bedrock throttling detected" in call["message"]
        assert "bedrock-model" in call["message"]
        assert "operation=invoke" in call["message"]

class TestBuildSystemPrompt:
    """Tests for HISAgent._build_system_prompt method."""

    def test_returns_default_prompt_when_user_prompt_is_none(self):
        result = HISAgent._build_system_prompt(
            None, "test-table", "sess-123", "agent-456"
        )
        assert isinstance(result, str)
        assert "test-table" in result
        assert "sess-123" in result
        assert "agent-456" in result
        # Ensure key DynamoDB reporting event types are present in the default template
        assert "START" in result
        assert "TOOLING" in result
        assert "MODEL_INVOCATION" in result
        assert "PROGRESS" in result
        assert "COMPLETION" in result
        assert "ERROR" in result

    def test_combines_with_string_user_prompt(self):
        user_prompt = "You are a helpful assistant."
        result = HISAgent._build_system_prompt(
            user_prompt, "test-table", "sess-123", "agent-456"
        )

        assert isinstance(result, str)
        assert "test-table" in result
        assert "sess-123" in result
        assert "agent-456" in result
        assert user_prompt in result

    def test_combines_with_list_user_prompt(self):
        user_prompt = [{"text": "You are a helpful assistant."}]
        result = HISAgent._build_system_prompt(
            user_prompt, "test-table", "sess-123", "agent-456"
        )

        assert isinstance(result, list)
        assert len(result) == 2
        assert "test-table" in result[0]["text"]
        assert "sess-123" in result[0]["text"]
        assert "agent-456" in result[0]["text"]
        assert result[1]["text"] == "You are a helpful assistant."

    def test_preserves_multiple_content_blocks(self):
        user_prompt = [
            {"text": "First instruction."},
            {"text": "Second instruction."},
        ]
        result = HISAgent._build_system_prompt(
            user_prompt, "test-table", "sess-123", "agent-456"
        )

        assert isinstance(result, list)
        assert len(result) == 3
        assert "test-table" in result[0]["text"]


class TestBuildTools:
    """Tests for HISAgent._build_tools method."""

    def test_returns_write_dynamo_when_tools_is_none(self):
        result = HISAgent._build_tools(None)
        assert len(result) == 1
        assert result[0] is write_dynamo

    def test_appends_write_dynamo_to_tools_list(self):
        user_tools = ["tool1", "tool2"]
        result = HISAgent._build_tools(user_tools)

        assert len(result) == 3
        assert result[0] == "tool1"
        assert result[1] == "tool2"
        assert result[2] is write_dynamo

    def test_handles_empty_tools_list(self):
        result = HISAgent._build_tools([])
        assert len(result) == 1
        assert result[0] is write_dynamo

    def test_does_not_modify_original_list(self):
        user_tools = ["tool1", "tool2"]
        original_length = len(user_tools)
        HISAgent._build_tools(user_tools)

        assert len(user_tools) == original_length
