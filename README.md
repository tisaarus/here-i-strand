# Here I Strand

Library that provides a [Strands](https://github.com/strands-agents) agent for AWS Bedrock Agent Core with DynamoDB status tracking, S3 session persistence, and a concurrent tool executor with per-tool timeouts (HIS: *here-i-strand*).

**Repository:** [github.com/tisaarus/here-i-strand](https://github.com/tisaarus/here-i-strand)

## Installation

```bash
pip install here-i-strand
# or with uv
uv add here-i-strand
```

## Quick start

You define how to load configuration (environment variables, your own settings module, etc.) and pass the values to `HISAgent`:

```python
import os
from his import HISAgent

agent = HISAgent(
    bucket_name=os.environ["BUCKET_NAME"],
    status_dynamo_table_name=os.environ["STATUS_DYNAMO_TABLE_NAME"],
    session_id="my-session-id",
    name=os.environ.get("AGENT_NAME", "my-agent"),
    agent_id=os.environ.get("AGENT_ID"),  # Optional: unique identifier for the agent instance
    tools=[...],
)
```

## Configuration

The library does not provide a settings layer. Pass `bucket_name`, `status_dynamo_table_name`, `session_id`, and optionally `name`, `agent_id` (and any other `HISAgent` arguments) from your own config:

- Environment variables
- A `.env` file loaded by your app (e.g. `python-dotenv`, `pydantic-settings`)
- Any other configuration source you use

## Project structure (development)

```
here-i-strand/
├── README.md
├── pyproject.toml
├── .env.example             # Example env vars (for your app)
│
├── his/                      # Main package
│   ├── __init__.py           # HISAgent, TimeoutConcurrentToolExecutor, event_loop_tracker, write_dynamo
│   ├── his.py                # HISAgent, TimeoutConcurrentToolExecutor, event_loop_tracker, write_dynamo
│   └── logging/
│       └── logging.py
│
└── tests/
    ├── conftest.py
    └── test_his.py
```

## Main components

- **`HISAgent`**: Strands agent with DynamoDB status tracking and optional S3-backed sessions. You pass `status_dynamo_table_name`, `session_id`, and optionally `bucket_name`, `name`, and `agent_id` so the tracker and session persistence use your table and bucket. Includes a default system prompt that enforces DynamoDB status reporting at key milestones. The `write_dynamo` tool is automatically added to the agent's tools.
- **`TimeoutConcurrentToolExecutor`**: Tool executor with a per-invocation timeout (default: 300s); if a tool exceeds the limit, an error is returned and execution continues with the rest.
- **`event_loop_tracker`**: Callback that writes event-loop milestones (`init_event_loop`, `result`, `force_stop`) to DynamoDB along with session and agent identifiers.
- **`write_dynamo`**: Strands tool for writing custom event records to DynamoDB for agent tracking and observability. Automatically included in `HISAgent`. Each call creates a new append-only event item identified by a composite key (`session_id`, `event_id`).
- **`DEFAULT_DYNAMODB_REPORTING_PROMPT_TEMPLATE`**: Template for the default system prompt that instructs the agent to report status updates to DynamoDB at key milestones such as `START`, `TOOLING`, `MODEL_INVOCATION`, `PROGRESS`, `COMPLETION`, and `ERROR`. The template is populated with the actual `table_name`, `session_id`, and `agent_id` values when the agent is created and explicitly instructs the agent to keep this reporting completely invisible to the end user.
- **`HISBedrockThrottlingLogger`**: Hook provider that listens for `ModelThrottledException` events and writes `BEDROCK_THROTTLING` records to DynamoDB for observability, independent of the system prompt.
- **`HISAgent.stream_async`**: Async streaming path that yields events from Strands and, on the final `result` event, persists `AGENT_STATS` and `RESULT` telemetry to DynamoDB.

## Default behavior

When you create an `HISAgent`, the following happens automatically:

1. **System prompt**: The `DEFAULT_DYNAMODB_REPORTING_PROMPT_TEMPLATE` is formatted with your `status_dynamo_table_name`, `session_id`, and `agent_id` and prepended to any custom system prompt you provide. It instructs the agent to report status at key milestones (`START`, `TOOLING`, `MODEL_INVOCATION`, `PROGRESS`, `COMPLETION`, `ERROR`) while keeping this reporting hidden from the user.
2. **Tools**: The `write_dynamo` tool is automatically added to your tools list, enabling the agent to write status updates to DynamoDB.
3. **Session persistence**: If you provide `bucket_name`, the agent uses `S3SessionManager` to persist session history. If `bucket_name` is omitted, no session manager is created.
4. **Event tracking**: The `event_loop_tracker` callback records event-loop milestones to DynamoDB.
5. **Invocation telemetry**: Both `__call__` and `stream_async` log `AGENT_STATS` and `RESULT` events to DynamoDB when an invocation completes.

## DynamoDB status events and schema

The library expects a DynamoDB table where status and telemetry events are written in an append-only fashion:

- **Partition key**: `session_id` (String)
- **Sort key**: `event_id` (String, UUID generated per event)

Each item also includes:

- `agent_id`: Agent instance identifier
- `evt_type`: Event type (e.g. `START`, `TOOLING`, `MODEL_INVOCATION`, `PROGRESS`, `BEDROCK_THROTTLING`, `COMPLETION`, `ERROR`, `EVENT_LOOP`, `AGENT_STATS`, `RESULT`)
- `evt_message`: Human-readable message describing the event or a JSON-encoded payload
- `evt_datetime`: ISO datetime of when the event was recorded

Typical high-level event types written by the agent are:

- `START`: Beginning of request processing.
- `TOOLING`: Before executing any external tool (includes tool name and purpose).
- `MODEL_INVOCATION`: Before invoking another model or sub-agent.
- `PROGRESS`: Significant progress or intermediate result checkpoints.
- `BEDROCK_THROTTLING`: Bedrock throttling errors (rate/concurrency limits), including retry context.
- `AGENT_STATS`: Aggregated usage metrics for the invocation (tokens, cache usage, user).
- `RESULT`: Final structured result of the agent run, stored as JSON when possible (falls back to a `{"raw": "<text>"}` wrapper when parsing fails).
- `COMPLETION`: Successful end of processing.
- `ERROR`: Unexpected errors with details for debugging.

## Tests

```bash
uv sync --all-groups
uv run pytest tests/ -v
```

## Authors

- [Lorenzo Pizarro Martin](mailto:lorenzo.pizarro.martin@gmail.com)
- [Antonio Calavia Robert](mailto:calavia88@gmail.com)
- [Manuel Sierra Lavado](mailto:yosierr@gmail.com)

## License

MIT. See [LICENSE](LICENSE).
