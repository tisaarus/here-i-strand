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
    tools=[...],
)
```

## Configuration

The library does not provide a settings layer. Pass `bucket_name`, `status_dynamo_table_name`, `session_id`, and optionally `name` (and any other `HISAgent` arguments) from your own config:

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
│   ├── __init__.py           # HISAgent, TimeoutConcurrentToolExecutor, event_loop_tracker, ping_status_task
│   ├── his.py                # HISAgent, TimeoutConcurrentToolExecutor, ping, event_loop_tracker
│   └── logging/
│       └── logging.py
│
└── tests/
    ├── conftest.py
    └── test_his.py
```

## Main components

- **`HISAgent`**: Strands agent with a DynamoDB ping thread and S3 sessions. You pass `status_dynamo_table_name`, `bucket_name`, `session_id`, and optionally `name` so the ping and tracker use your table and bucket.
- **`TimeoutConcurrentToolExecutor`**: Tool executor with a per-invocation timeout; if a tool exceeds the limit, an error is returned and execution continues with the rest.
- **`event_loop_tracker`**: Callback that writes event-loop milestones (init, start, message, result, force_stop) to DynamoDB and stops the ping when done.

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
