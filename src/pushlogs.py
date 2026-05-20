# =========================================================
# log_utils.py — Shared Live Logging System (no circular import)
# =========================================================
from fastapi.responses import StreamingResponse
import asyncio

# Global buffer for logs
live_logs = []

def push_log(msg: str):
    """Push message to console and SSE stream."""
    live_logs.append(msg)
    print(msg)

async def event_generator():
    """Yields log messages for SSE clients."""
    last = 0
    while True:
        if len(live_logs) > last:
            for i in range(last, len(live_logs)):
                yield f"data: {live_logs[i]}\n\n"
            last = len(live_logs)
        await asyncio.sleep(1)

def get_stream_response():
    """Return StreamingResponse for /v1/automation/logs endpoint."""
    return StreamingResponse(event_generator(), media_type="text/event-stream")