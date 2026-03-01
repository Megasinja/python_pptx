from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import signal
import sys
import textwrap
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
JUPYTER_MESSAGE_PREFIX = "__JUPYTER_MSG__:"
WORKER_RESULT_PREFIX = "__WORKER_RESULT__:"

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
    run_result_futures: dict[str, asyncio.Future[int]],
) -> None:
    if pipe is None:
        return
    while True:
        chunk = await pipe.readline()
        if not chunk:
            break
        decoded = chunk.decode("utf-8", errors="replace")

        if stream_type == "stdout" and decoded.startswith(JUPYTER_MESSAGE_PREFIX):
            marker_payload = decoded[len(JUPYTER_MESSAGE_PREFIX) :].strip()
            message_type, _, encoded_payload = marker_payload.partition(":")
            if encoded_payload:
                with contextlib.suppress(Exception):
                    payload_json = base64.b64decode(encoded_payload.encode("ascii")).decode("utf-8")
                    payload = json.loads(payload_json)
                    if message_type == "display_data":
                        await safe_send_json(
                            websocket,
                            {
                                "type": "display_data",
                                "data": payload.get("data", {}),
                                "metadata": payload.get("metadata", {}),
                            },
                        )
                        continue

        if stream_type == "stdout" and decoded.startswith(WORKER_RESULT_PREFIX):
            marker_payload = decoded[len(WORKER_RESULT_PREFIX) :].strip()
            run_id, _, return_code_text = marker_payload.partition(":")
            if run_id:
                return_code = 1
                with contextlib.suppress(Exception):
                    return_code = int(return_code_text)
                run_future = run_result_futures.get(run_id)
                if run_future is not None and not run_future.done():
                    run_future.set_result(return_code)
            continue

        await safe_send_json(
            websocket,
            {
                "type": stream_type,
                "data": decoded,
            },
        )


def build_worker_script() -> str:
    return textwrap.dedent(
        f"""
        import base64
        import io
        import json
        import sys
        import traceback

        JUPYTER_MESSAGE_PREFIX = {json.dumps(JUPYTER_MESSAGE_PREFIX)}
        WORKER_RESULT_PREFIX = {json.dumps(WORKER_RESULT_PREFIX)}

        def _emit_message(message_type, payload):
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            encoded = base64.b64encode(raw).decode("ascii")
            print(JUPYTER_MESSAGE_PREFIX + message_type + ":" + encoded)

        runtime_globals = {{"__name__": "__main__"}}

        for raw_line in sys.stdin:
            run_id = "unknown"
            return_code = 1

            try:
                payload = json.loads(raw_line)
                run_id = str(payload.get("id", "unknown"))
                user_code = str(payload.get("code", ""))
            except Exception:
                traceback.print_exc()
                print(WORKER_RESULT_PREFIX + str(run_id) + ":1")
                continue

            try:
                exec(compile(user_code, "<user_code>", "exec"), runtime_globals, runtime_globals)
                return_code = 0
            except KeyboardInterrupt:
                return_code = 130
            except SystemExit as exc:
                return_code = int(exc.code) if isinstance(exc.code, int) else 1
            except Exception:
                return_code = 1
                traceback.print_exc()

            try:
                import matplotlib.pyplot as plt
                from matplotlib import _pylab_helpers

                managers = _pylab_helpers.Gcf.get_all_fig_managers()
                for index, manager in enumerate(managers, start=1):
                    figure = manager.canvas.figure
                    buffer = io.BytesIO()
                    figure.savefig(buffer, format="png", bbox_inches="tight")
                    encoded_png = base64.b64encode(buffer.getvalue()).decode("ascii")

                    _emit_message(
                        "display_data",
                        {{
                            "data": {{
                                "image/png": encoded_png,
                                "text/plain": "<Figure " + str(index) + ">",
                            }},
                            "metadata": {{}},
                        }},
                    )

                if managers:
                    plt.close("all")
            except Exception:
                # Keep normal execution behavior even if display export fails.
                pass

            print(WORKER_RESULT_PREFIX + run_id + ":" + str(return_code))
            # Ensure marker and logs are flushed quickly.
            sys.stdout.flush()
            sys.stderr.flush()
        """
    )


async def shutdown_runtime_process(
    process_holder: dict[str, Any],
    stream_tasks_holder: dict[str, asyncio.Task[None] | None],
    run_result_futures: dict[str, asyncio.Future[int]],
) -> None:
    process = process_holder.get("process")
    if process is not None and process.returncode is None:
        process.kill()
        with contextlib.suppress(asyncio.TimeoutError, Exception):
            await asyncio.wait_for(process.wait(), timeout=1.0)
    process_holder["process"] = None

    for key in ("stdout_task", "stderr_task"):
        task = stream_tasks_holder.get(key)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=1.0)
            stream_tasks_holder[key] = None

    for future in run_result_futures.values():
        if not future.done():
            future.cancel()
    run_result_futures.clear()


async def ensure_runtime_process(
    websocket: WebSocket,
    process_holder: dict[str, Any],
    stream_tasks_holder: dict[str, asyncio.Task[None] | None],
    run_result_futures: dict[str, asyncio.Future[int]],
) -> bool:
    process = process_holder.get("process")
    stdout_task = stream_tasks_holder.get("stdout_task")
    stderr_task = stream_tasks_holder.get("stderr_task")

    if (
        process is not None
        and process.returncode is None
        and stdout_task is not None
        and not stdout_task.done()
        and stderr_task is not None
        and not stderr_task.done()
    ):
        return True

    await shutdown_runtime_process(process_holder, stream_tasks_holder, run_result_futures)

    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            "-c",
            build_worker_script(),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception:
        await safe_send_json(
            websocket,
            {"type": "error", "message": "Failed to start Python runtime process."},
        )
        return False

    process_holder["process"] = process
    stream_tasks_holder["stdout_task"] = asyncio.create_task(
        stream_pipe(websocket, process.stdout, "stdout", run_result_futures)
    )
    stream_tasks_holder["stderr_task"] = asyncio.create_task(
        stream_pipe(websocket, process.stderr, "stderr", run_result_futures)
    )
    return True


async def run_python_code(
    websocket: WebSocket,
    code: str,
    process_holder: dict[str, Any],
    stream_tasks_holder: dict[str, asyncio.Task[None] | None],
    run_result_futures: dict[str, asyncio.Future[int]],
    abort_event: asyncio.Event,
) -> None:
    if not code.strip():
        await safe_send_json(
            websocket,
            {"type": "error", "message": "Python code is empty."},
        )
        return

    if not await ensure_runtime_process(websocket, process_holder, stream_tasks_holder, run_result_futures):
        return

    process = process_holder.get("process")
    if process is None or process.returncode is not None or process.stdin is None:
        await safe_send_json(
            websocket,
            {"type": "error", "message": "Python runtime is not available."},
        )
        return

    run_id = str(process_holder.get("next_run_id", 1))
    process_holder["next_run_id"] = int(process_holder.get("next_run_id", 1)) + 1
    loop = asyncio.get_running_loop()
    run_future: asyncio.Future[int] = loop.create_future()
    run_result_futures[run_id] = run_future

    await safe_send_json(websocket, {"type": "status", "state": "started"})

    try:
        payload_line = json.dumps({"id": run_id, "code": code}, ensure_ascii=False) + "\n"
        process.stdin.write(payload_line.encode("utf-8"))
        await process.stdin.drain()
    except Exception:
        run_result_futures.pop(run_id, None)
        await safe_send_json(
            websocket,
            {"type": "error", "message": "Failed to submit code to Python runtime."},
        )
        return

    try:
        return_code = await run_future
    finally:
        run_result_futures.pop(run_id, None)

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
    process_holder: dict[str, Any] = {"process": None, "next_run_id": 1}
    stream_tasks_holder: dict[str, asyncio.Task[None] | None] = {
        "stdout_task": None,
        "stderr_task": None,
    }
    run_result_futures: dict[str, asyncio.Future[int]] = {}
    run_task: asyncio.Task[None] | None = None
    abort_event = asyncio.Event()
    await ensure_runtime_process(websocket, process_holder, stream_tasks_holder, run_result_futures)

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
                        stream_tasks_holder=stream_tasks_holder,
                        run_result_futures=run_result_futures,
                        abort_event=abort_event,
                    )
                )
                continue

            if message_type == "abort":
                abort_event.set()
                process = process_holder.get("process")
                if process is not None and process.returncode is None:
                    with contextlib.suppress(Exception):
                        process.send_signal(signal.SIGINT)
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
        await shutdown_runtime_process(process_holder, stream_tasks_holder, run_result_futures)
        if run_task is not None:
            with contextlib.suppress(Exception):
                await run_task
