"""
HIS (here-i-strand) agent module.

Provides a custom Strands agent with DynamoDB status tracking, S3 session persistence,
and a concurrent tool executor with per-tool timeouts.
"""
import threading
import time
from datetime import datetime
from functools import partial
from typing import Union, Callable, Any, Mapping

import boto3
from pydantic import BaseModel
from strands import Agent
from strands.event_loop._retry import ModelRetryStrategy
from strands.agent import ConversationManager
from strands.agent.agent import _DefaultCallbackHandlerSentinel
from strands.agent.state import AgentState
from strands.hooks import HookProvider
from strands.models import Model
from strands.session import S3SessionManager
from strands.tools.executors._executor import ToolExecutor
from strands.types.content import Messages, SystemContentBlock
from strands.types.traces import AttributeValue

import asyncio
from strands.tools.executors import ConcurrentToolExecutor
from strands.tools.executors._executor import ToolExecutor
from strands.types._events import ToolResultEvent
from strands.types.tools import ToolResult, ToolUse
from strands.tools.structured_output._structured_output_context import StructuredOutputContext

from his.logging.logging import _init_logger

logger = _init_logger()
logger.info("Starting agent")


def ping_status_task(status_dynamo_table_name: str, session_id: str, stop_event: threading.Event):
    """
    Background task that periodically updates the agent status in DynamoDB.
    Writes 'running' every 20 seconds until stop_event is set, then writes 'ended'.
    """
    try:
        while not stop_event.is_set():
            logger.info(f" PING Starting - Session {session_id} ")
            try:
                client = boto3.client('dynamodb', region_name='eu-central-1')
                client.update_item(
                    TableName=status_dynamo_table_name,
                    Key={
                        'session_id': {'S': session_id}
                    },
                    UpdateExpression=f"SET ping_datetime = :last_updated, ping_status = :status",
                    ExpressionAttributeValues={
                        ':status': {'S': 'running'},
                        ':last_updated': {'S': str(datetime.now())},
                    }
                )
                logger.info(f" PING Ending - Session {session_id} ")
            except Exception as e:
                logger.error(f"Error pinging localhost:8080/ping: {e}")

            time.sleep(20)
        logger.info(f" PING Stopped - Session {session_id} ")
        client = boto3.client('dynamodb', region_name='eu-central-1')
        client.update_item(
            TableName=status_dynamo_table_name,
            Key={
                'session_id': {'S': session_id}
            },
            UpdateExpression=f"SET ping_datetime = :last_updated, ping_status = :status",
            ExpressionAttributeValues={
                ':status': {'S': 'ended'},
                ':last_updated': {'S': str(datetime.now())},
            }
        )
    except Exception as e:
        logger.error(f"Error pinging localhost:8080/ping: {e}")


def event_loop_tracker(**kwargs):
    """
    Callback that records event-loop milestones (init, start, message, result, force_stop)
    in DynamoDB. Stops the ping task when result or force_stop is received.
    """
    stop_ping_event: threading.Event = kwargs.get("stop_ping_event")
    field_to_update = None
    if kwargs.get("init_event_loop"):
        field_to_update = "init_event_loop"
    elif kwargs.get("start_event_loop"):
        field_to_update = "start_event_loop"
    elif "message" in kwargs:
        field_to_update = "message"
    elif "result" in kwargs:
        field_to_update = "result"
        stop_ping_event.set()
    elif kwargs.get("force_stop"):
        field_to_update = "force_stop"
        stop_ping_event.set()

    if field_to_update:
        client = boto3.client('dynamodb', region_name='eu-central-1')
        client.update_item(
            TableName=kwargs.get("status_dynamo_table_name", "AgentCoreAgentStatus"),
            Key={
                'session_id': {'S': kwargs.get("session_id", "default")}
            },
            UpdateExpression=f"SET agent_id = :ai, evt_{field_to_update} = :val,  evt_last_updated = :time, evt_agent_name = :name",
            ExpressionAttributeValues={
                ':ai': {'S': kwargs.get("agent_id", "default")},
                ':val': {'BOOL': True},
                ':time': {'S': str(datetime.now())},
                ':name': {'S': kwargs.get("agent_name", "default")}
            }
        )


class HISAgent(Agent):
    """
    Strands agent with DynamoDB status tracking and S3 session persistence.
    Starts a daemon thread that pings status to DynamoDB and stops it when the event loop finishes.
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
            args=(status_dynamo_table_name, session_id, stop_ping_event,),
            daemon=True
        )
        super().__init__(
            model=model,
            messages=messages,
            tools=tools,
            system_prompt=system_prompt,
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
                region_name="eu-central-1",
                prefix=f"ac-sessions/{name}"
            ),
            tool_executor=tool_executor,
            retry_strategy=retry_strategy,
        )
        ping_status_task_thread.start()


class TimeoutConcurrentToolExecutor(ConcurrentToolExecutor):
    """
    Concurrent tool executor with a per-tool timeout.
    If a tool exceeds timeout_seconds, an error result is returned and execution continues.
    """

    def __init__(self, timeout_seconds: float = 300.0) -> None:
        self.timeout_seconds = timeout_seconds

    async def _run_tool_stream(
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
