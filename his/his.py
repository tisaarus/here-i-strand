"""
HIS (here-i-strand) agent module.

Provides a custom Strands agent with DynamoDB status tracking, S3 session persistence,
and a concurrent tool executor with per-tool timeouts.
"""
import asyncio
import threading
import time
import uuid
from datetime import datetime
from functools import partial
from typing import Union, Callable, Any, Mapping

import boto3
from pydantic import BaseModel
from strands import Agent, tool
from strands.agent import ConversationManager
from strands.agent.agent import _DefaultCallbackHandlerSentinel
from strands.agent.state import AgentState
from strands.event_loop._retry import ModelRetryStrategy
from strands.hooks import HookProvider
from strands.models import Model
from strands.session import S3SessionManager
from strands.tools import ToolProvider
from strands.tools.executors import ConcurrentToolExecutor
from strands.tools.executors._executor import ToolExecutor
from strands.types._events import ToolResultEvent
from strands.types.content import Messages, SystemContentBlock
from strands.types.tools import ToolResult, ToolUse
from strands.types.traces import AttributeValue

from his.logging.logging import _init_logger

logger = _init_logger()
logger.info("Starting agent")


@tool
def write_dynamo(
        table_name: str,
        agent_id: str,
        session_id: str,
        event_type: str,
        message: str
) -> None:
    """
    Write an event record to DynamoDB for agent tracking and observability.

    This tool creates a new item in the specified DynamoDB table to record
    agent events such as status updates, milestones, or custom messages.
    Each call creates a new timestamped record (append-only pattern) with a 
    unique event_id (UUID).

    The DynamoDB table should have the following schema:
    - Partition Key: session_id (String)
    - Sort Key: event_id (String) - UUID generated automatically

    Args:
        table_name: The DynamoDB table name where the event will be stored.
        agent_id: Unique identifier for the agent instance.
        session_id: Unique identifier for the current session.
        event_type: Type of event being recorded (e.g., 'STATUS', 'MILESTONE', 'ERROR').
        message: Descriptive message or payload for the event.

    Returns:
        None

    Raises:
        Exception: If the DynamoDB put_item operation fails.

    Example:
        write_dynamo(
            table_name="agent-events",
            agent_id="agent-123",
            session_id="sess-456",
            event_type="STATUS",
            message="Processing started"
        )
    """
    try:
        client = boto3.client('dynamodb')
        client.put_item(
            TableName=table_name,
            Item={
                'session_id': {'S': session_id},
                'event_id': {'S': str(uuid.uuid4())},
                'agent_id': {'S': agent_id},
                'evt_type': {'S': event_type},
                'evt_message': {'S': message},
                'evt_datetime': {'S': str(datetime.now())}
            })
    except Exception as e:
        logger.error(f"Error writing to DynamoDB for session {session_id}: {e}")


def ping_status_task(status_dynamo_table_name: str, session_id: str, agent_id: str, stop_event: threading.Event):
    """
    Background task that periodically updates the agent status in DynamoDB.

    Writes a 'running' status every 20 seconds until stop_event is set,
    then writes a 'finished' status before exiting.

    Args:
        status_dynamo_table_name: DynamoDB table name for status tracking.
        session_id: Unique identifier for the current session.
        agent_id: Unique identifier for the agent instance.
        stop_event: Threading event to signal when to stop the ping loop.
    """
    try:
        while not stop_event.is_set():
            logger.info(f"PING Starting - Session {session_id}")
            try:
                client = boto3.client('dynamodb')
                client.put_item(TableName=status_dynamo_table_name, Item={
                    'session_id': {'S': session_id},
                    'event_id': {'S': str(uuid.uuid4())},
                    'agent_id': {'S': agent_id},
                    'evt_type': {'S': 'PING'},
                    'evt_message': {'S': 'running'},
                    'evt_datetime': {'S': str(datetime.now())}
                })
                logger.info(f"PING Completed - Session {session_id}")
            except Exception as e:
                logger.error(f"Error updating DynamoDB ping status for session {session_id}: {e}")

            time.sleep(20)
        logger.info(f"PING Stopped - Session {session_id}")
        client = boto3.client('dynamodb')
        client.put_item(TableName=status_dynamo_table_name, Item={
            'session_id': {'S': session_id},
            'event_id': {'S': str(uuid.uuid4())},
            'agent_id': {'S': agent_id},
            'evt_type': {'S': 'PING'},
            'evt_message': {'S': 'finished'},
            'evt_datetime': {'S': str(datetime.now())}
        })
    except Exception as e:
        logger.error(f"Error in ping_status_task for session {session_id}: {e}")


def event_loop_tracker(**kwargs):
    """
    Callback that records event-loop milestones in DynamoDB.

    Records the following milestones: init_event_loop, start_event_loop,
    message, result, and force_stop. Stops the ping task when result
    or force_stop is received.

    Kwargs:
        stop_ping_event: Threading event to signal ping task termination.
        status_dynamo_table_name: DynamoDB table name (default: "AgentCoreAgentStatus").
        session_id: Session identifier (default: "default").
        agent_id: Agent identifier (default: "default").
        agent_name: Agent name for tracking (default: "default").
        init_event_loop: If True, records initialization milestone.
        start_event_loop: If True, records start milestone.
        message: If present, records message milestone.
        result: If present, records result milestone and stops ping.
        force_stop: If True, records force_stop milestone and stops ping.
    """
    stop_ping_event: threading.Event = kwargs.get("stop_ping_event")
    field_to_update = None
    if kwargs.get("init_event_loop"):
        field_to_update = "init_event_loop"
    elif "result" in kwargs:
        field_to_update = "result"
        stop_ping_event.set()
    elif kwargs.get("force_stop"):
        field_to_update = "force_stop"
        stop_ping_event.set()

    if field_to_update:
        client = boto3.client('dynamodb')
        client.put_item(
            TableName=kwargs.get("status_dynamo_table_name", "AgentCoreAgentStatus"),
            Item={
                'session_id': {'S': kwargs.get("session_id", "default")},
                'event_id': {'S': str(uuid.uuid4())},
                'agent_id': {'S': kwargs.get("agent_id", "default")},
                'evt_type': {'S': 'EVENT_LOOP'},
                'evt_message': {'S': field_to_update},
                'evt_datetime': {'S': str(datetime.now())}
            })


DEFAULT_DYNAMODB_REPORTING_PROMPT_TEMPLATE = """
=== DYNAMODB REPORTING (Key Milestones) - MANDATORY ===
CRITICAL: You MUST update the status in DynamoDB using the write_dynamo tool to notify progress at each of these moments. This is NOT optional.

IMPORTANT: This is an INTERNAL process. NEVER mention, explain, or expose any information about DynamoDB reporting to the user. The user should not know about these status updates, the write_dynamo tool, or any tracking mechanism. Keep all reporting completely invisible to the user.

Use these EXACT values for write_dynamo parameters:
- table_name: "{table_name}"
- agent_id: "{agent_id}"
- session_id: "{session_id}"
- event_type: Type of event (see milestones below)
- message: Descriptive message about the current status

MANDATORY MILESTONES where you must call write_dynamo:
- START: event_type="START", message="Processing user request"
- TOOLING/PROGRESS: event_type="PROGRESS", message="<descriptive message for monitoring>"
  NOTE: Progress messages must be detailed and descriptive as they are used for process monitoring. 
  Include: what action is being performed, which tool is being used, and relevant context.
  Example: "Executing search_database tool to retrieve user records", "Calling external API to fetch weather data"
- COMPLETION: event_type="COMPLETION", message="Process completed successfully"
- IF ERROR: event_type="ERROR", message="<detailed error description for debugging>"

FORBIDDEN: 
- Continuing to the next step without having called write_dynamo at each critical milestone.
- Mentioning or explaining the DynamoDB reporting process to the user.
"""


class HISAgent(Agent):
    """
    Strands agent with DynamoDB status tracking and S3 session persistence.

    Extends the base Strands Agent with:
    - A daemon thread that pings status to DynamoDB every 20 seconds
    - S3-based session persistence for conversation history
    - Event loop tracking via callback handler
    - Default system prompt enforcing DynamoDB status reporting

    The ping thread automatically stops when the event loop completes
    (result or force_stop received).

    Args:
        bucket_name: S3 bucket name for session persistence.
        status_dynamo_table_name: DynamoDB table name for status tracking.
        session_id: Unique identifier for the current session.
        model: Model instance or model ID string.
        messages: Initial conversation messages.
        tools: List of tools available to the agent.
        system_prompt: System prompt for the agent (will be combined with
            the default DynamoDB reporting prompt).
        structured_output_model: Pydantic model for structured output.
        callback_handler: Custom callback handler (defaults to event_loop_tracker).
        conversation_manager: Custom conversation manager.
        record_direct_tool_call: Whether to record direct tool calls.
        load_tools_from_directory: Whether to load tools from directory.
        trace_attributes: Attributes for tracing.
        agent_id: Unique identifier for the agent instance.
        name: Human-readable name for the agent.
        description: Description of the agent's purpose.
        state: Initial agent state.
        hooks: List of hook providers.
        tool_executor: Custom tool executor.
        retry_strategy: Strategy for retrying failed model calls.
    """

    def __init__(
            self,
            bucket_name: str,
            status_dynamo_table_name: str,
            session_id: str,
            model: Model | str | None = None,
            messages: Messages | None = None,
            tools: list[Union[str, dict[str, str], "ToolProvider", Any]] | None = None,
            system_prompt: str | list[SystemContentBlock] | None = None,
            structured_output_model: type[BaseModel] | None = None,
            callback_handler: Callable[
                                  ..., Any] | _DefaultCallbackHandlerSentinel | None = None,
            conversation_manager: ConversationManager | None = None,
            record_direct_tool_call: bool = True,
            load_tools_from_directory: bool = False,
            trace_attributes: Mapping[str, AttributeValue] | None = None,
            *,
            agent_id: str | None = None,
            name: str | None = None,
            description: str | None = None,
            state: AgentState | dict | None = None,
            hooks: list[HookProvider] | None = None,
            tool_executor: ToolExecutor | None = None,
            retry_strategy: ModelRetryStrategy | None = None,
    ):
        stop_ping_event = threading.Event()
        ping_status_task_thread = threading.Thread(
            target=ping_status_task,
            args=(status_dynamo_table_name, session_id, agent_id or "default", stop_ping_event),
            daemon=True
        )

        combined_system_prompt = self._build_system_prompt(
            system_prompt,
            status_dynamo_table_name,
            session_id,
            agent_id or "default"
        )
        combined_tools = self._build_tools(tools)
        super().__init__(
            model=model,
            messages=messages,
            tools=combined_tools,
            system_prompt=combined_system_prompt,
            structured_output_model=structured_output_model,
            callback_handler=callback_handler or partial(
                event_loop_tracker,
                stop_ping_event=stop_ping_event,
                status_dynamo_table_name=status_dynamo_table_name,
                bucket_name=bucket_name,
                session_id=session_id,
                agent_name=name,
                agent_id=agent_id
            ),
            conversation_manager=conversation_manager,
            record_direct_tool_call=record_direct_tool_call,
            load_tools_from_directory=load_tools_from_directory,
            trace_attributes=trace_attributes,
            agent_id=agent_id,
            name=name,
            description=description,
            state=state,
            hooks=hooks,
            session_manager=S3SessionManager(
                session_id=session_id,
                bucket=bucket_name,
                prefix=f"ac-sessions/{name}"
            ),
            tool_executor=tool_executor,
            retry_strategy=retry_strategy,
        )
        ping_status_task_thread.start()

    @staticmethod
    def _build_tools(
            tools: list[Union[str, dict[str, str], "ToolProvider", Any]] | None
    ) -> list[Union[str, dict[str, str], "ToolProvider", Any]]:
        """
        Build the tools list by appending the write_dynamo tool.

        Args:
            tools: User-provided list of tools (can be None).

        Returns:
            List of tools with write_dynamo appended for DynamoDB status reporting.
        """
        if tools is None:
            return [write_dynamo]
        return list(tools) + [write_dynamo]

    @staticmethod
    def _build_system_prompt(
            system_prompt: str | list[SystemContentBlock] | None,
            table_name: str,
            session_id: str,
            agent_id: str,
    ) -> str | list[SystemContentBlock]:
        """
        Combine the default DynamoDB reporting prompt with the user-provided prompt.

        Args:
            system_prompt: The user-provided system prompt (string or list of content blocks).
            table_name: DynamoDB table name for status tracking.
            session_id: Unique identifier for the current session.
            agent_id: Unique identifier for the agent instance.

        Returns:
            Combined system prompt with DynamoDB reporting instructions prepended,
            including the actual table_name, session_id, and agent_id values.
        """
        default_prompt = DEFAULT_DYNAMODB_REPORTING_PROMPT_TEMPLATE.format(
            table_name=table_name,
            session_id=session_id,
            agent_id=agent_id,
        )

        if system_prompt is None:
            return default_prompt

        if isinstance(system_prompt, str):
            return f"{default_prompt}\n\n{system_prompt}"

        default_block: SystemContentBlock = {"text": default_prompt}
        return [default_block] + list(system_prompt)


class TimeoutConcurrentToolExecutor(ConcurrentToolExecutor):
    """
    Concurrent tool executor with a per-tool timeout.

    Extends ConcurrentToolExecutor to add timeout handling for individual tools.
    If a tool exceeds the configured timeout, an error result is returned and
    execution continues with remaining tools.

    Args:
        timeout_seconds: Maximum time in seconds to wait for each tool execution.
            Defaults to 300.0 (5 minutes).

    Example:
        executor = TimeoutConcurrentToolExecutor(timeout_seconds=60.0)
        agent = HISAgent(..., tool_executor=executor)
    """

    def __init__(self, timeout_seconds: float = 300.0) -> None:
        self.timeout_seconds = timeout_seconds

    @staticmethod
    async def _run_tool_stream(
            agent: "Agent",
            tool_use: ToolUse,
            tool_results: list[ToolResult],
            cycle_trace: Any,
            cycle_span: Any,
            invocation_state: dict[str, Any],
            task_id: int,
            task_queue: asyncio.Queue,
            task_event: asyncio.Event,
            structured_output_context: "StructuredOutputContext | None",
    ) -> None:
        """Consume the tool stream and enqueue events so the caller can wrap with asyncio.wait_for."""
        events = ToolExecutor._stream_with_trace(
            agent,
            tool_use,
            tool_results,
            cycle_trace,
            cycle_span,
            invocation_state,
            structured_output_context,
        )
        async for event in events:
            task_queue.put_nowait((task_id, event))
            await task_event.wait()
            task_event.clear()

    async def _task(
            self,
            agent: "Agent",
            tool_use: ToolUse,
            tool_results: list[ToolResult],
            cycle_trace: Any,
            cycle_span: Any,
            invocation_state: dict[str, Any],
            task_id: int,
            task_queue: asyncio.Queue,
            task_event: asyncio.Event,
            stop_event: object,
            structured_output_context: "StructuredOutputContext | None",
    ) -> None:
        """Run a single tool with timeout; on timeout, append an error result and continue."""
        tool_use_id = str(tool_use.get("toolUseId", ""))
        tool_name = tool_use.get("name", "unknown")
        try:
            await asyncio.wait_for(
                self._run_tool_stream(
                    agent,
                    tool_use,
                    tool_results,
                    cycle_trace,
                    cycle_span,
                    invocation_state,
                    task_id,
                    task_queue,
                    task_event,
                    structured_output_context,
                ),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Tool %s (%s) timed out after %s seconds",
                tool_name,
                tool_use_id,
                self.timeout_seconds,
            )
            # Return a structured error so the agent can continue with other tools
            error_result: ToolResult = {
                "toolUseId": tool_use_id,
                "status": "error",
                "content": [
                    {"text": f"Tool execution timed out after {self.timeout_seconds}s."}
                ],
            }
            tool_results.append(error_result)
            task_queue.put_nowait((task_id, ToolResultEvent(error_result)))
        finally:
            task_queue.put_nowait((task_id, stop_event))
