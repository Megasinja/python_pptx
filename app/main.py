from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import textwrap
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
EXEC_TIMEOUT_SECONDS = 10
PLOT_MARKER_PREFIX = "__PLOT_PNG__:"

app = FastAPI(title="Python Training Prototype")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "pythonVersion": sys.version.split()[0],
        "pythonExecutable": sys.executable,
    }


async def safe_send_json(websocket: WebSocket, payload: dict[str, Any]) -> None:
    try:
        await websocket.send_json(payload)
    except Exception:
        # Ignore send failures after disconnect.
        pass


async def stream_pipe(
    websocket: WebSocket,
    pipe: asyncio.StreamReader | None,
    stream_type: str,
) -> None:
    if pipe is None:
        return
    while True:
        chunk = await pipe.readline()
        if not chunk:
            break
        decoded = chunk.decode("utf-8", errors="replace")

        if stream_type == "stdout" and decoded.startswith(PLOT_MARKER_PREFIX):
            marker_payload = decoded[len(PLOT_MARKER_PREFIX) :].strip()
            index_str, _, plot_data = marker_payload.partition(":")
            if plot_data:
                try:
                    plot_index: int | None = int(index_str)
                except ValueError:
                    plot_index = None
                await safe_send_json(
                    websocket,
                    {"type": "plot", "index": plot_index, "data": plot_data},
                )
                continue

        await safe_send_json(
            websocket,
            {
                "type": stream_type,
                "data": decoded,
            },
        )


def build_exec_script(user_code: str) -> str:
    encoded_code = json.dumps(user_code)
    return textwrap.dedent(
        f"""
        import base64
        import io
        import os
        import sys
        import traceback

        os.environ.setdefault("MPLBACKEND", "Agg")
        USER_CODE = {encoded_code}
        user_globals = {{"__name__": "__main__"}}

        try:
            exec(compile(USER_CODE, "<user_code>", "exec"), user_globals, user_globals)
        except Exception:
            traceback.print_exc()
            raise SystemExit(1)

        try:
            import matplotlib.pyplot as plt
            from matplotlib import _pylab_helpers

            managers = _pylab_helpers.Gcf.get_all_fig_managers()
            for index, manager in enumerate(managers, start=1):
                figure = manager.canvas.figure
                buffer = io.BytesIO()
                figure.savefig(buffer, format="png", bbox_inches="tight")
                encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
                print("{PLOT_MARKER_PREFIX}" + str(index) + ":" + encoded)

            if managers:
                plt.close("all")
        except Exception:
            # Ignore plot export failures to keep normal execution behavior.
            pass
        """
    )


async def run_python_code(
    websocket: WebSocket,
    code: str,
    process_holder: dict[str, asyncio.subprocess.Process | None],
    abort_event: asyncio.Event,
) -> None:
    if not code.strip():
        await safe_send_json(
            websocket,
            {"type": "error", "message": "Python code is empty."},
        )
        return

    await safe_send_json(websocket, {"type": "status", "state": "started"})
    exec_script = build_exec_script(code)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        "-c",
        exec_script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    process_holder["process"] = process

    stdout_task = asyncio.create_task(stream_pipe(websocket, process.stdout, "stdout"))
    stderr_task = asyncio.create_task(stream_pipe(websocket, process.stderr, "stderr"))

    timed_out = False
    try:
        return_code = await asyncio.wait_for(process.wait(), timeout=EXEC_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        timed_out = True
        abort_event.set()
        process.kill()
        return_code = await process.wait()

    await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
    process_holder["process"] = None

    if timed_out:
        await safe_send_json(
            websocket,
            {
                "type": "status",
                "state": "timeout",
                "message": f"Execution exceeded {EXEC_TIMEOUT_SECONDS} seconds.",
                "returnCode": return_code,
            },
        )
        return

    if abort_event.is_set():
        await safe_send_json(
            websocket,
            {"type": "status", "state": "aborted", "returnCode": return_code},
        )
        return

    await safe_send_json(
        websocket,
        {"type": "status", "state": "finished", "returnCode": return_code},
    )


@app.websocket("/ws/run")
async def run_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    process_holder: dict[str, asyncio.subprocess.Process | None] = {"process": None}
    run_task: asyncio.Task[None] | None = None
    abort_event = asyncio.Event()

    try:
        while True:
            raw_message = await websocket.receive_text()
            try:
                payload = json.loads(raw_message)
            except json.JSONDecodeError:
                await safe_send_json(
                    websocket,
                    {"type": "error", "message": "Invalid JSON payload."},
                )
                continue

            message_type = payload.get("type")
            if message_type == "run":
                if run_task is not None and not run_task.done():
                    await safe_send_json(
                        websocket,
                        {"type": "error", "message": "Execution is already running."},
                    )
                    continue

                abort_event = asyncio.Event()
                run_task = asyncio.create_task(
                    run_python_code(
                        websocket=websocket,
                        code=str(payload.get("code", "")),
                        process_holder=process_holder,
                        abort_event=abort_event,
                    )
                )
                continue

            if message_type == "abort":
                abort_event.set()
                process = process_holder.get("process")
                if process is not None and process.returncode is None:
                    process.terminate()
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(process.wait(), timeout=1.0)
                    if process.returncode is None:
                        process.kill()
                await safe_send_json(websocket, {"type": "status", "state": "aborting"})
                continue

            await safe_send_json(
                websocket,
                {"type": "error", "message": f"Unknown message type: {message_type!r}"},
            )
    except WebSocketDisconnect:
        pass
    finally:
        abort_event.set()
        process = process_holder.get("process")
        if process is not None and process.returncode is None:
            process.kill()
            with contextlib.suppress(Exception):
                await process.wait()
        if run_task is not None:
            with contextlib.suppress(Exception):
                await run_task
