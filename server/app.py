from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parent.parent
BRIDGE_SCRIPT = Path(__file__).with_name("room_bridge.js")
ROOM_CODE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
MAX_CONNECTIONS = 16
MAX_MESSAGE_BYTES = 8192
AI_TURN_DELAY_SECONDS = 1.2
BRIDGE_TIMEOUT_SECONDS = 60.0
MUTATING_MESSAGES = {
    "join",
    "start",
    "action",
    "name",
    "rematch",
    "takeover",
    "addAI",
    "removeAI",
    "aiLevel",
    "shuffle",
}


class BridgeError(RuntimeError):
    pass


class RoomStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS rooms (
                code TEXT PRIMARY KEY,
                snapshot TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        self._db.commit()

    def load(self, code: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT snapshot FROM rooms WHERE code = ?", (code,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, code: str, snapshot: dict[str, Any]) -> None:
        payload = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
        self._db.execute(
            """
            INSERT INTO rooms(code, snapshot, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                snapshot = excluded.snapshot,
                updated_at = excluded.updated_at
            """,
            (code, payload, int(time.time())),
        )
        self._db.commit()

    def close(self) -> None:
        self._db.close()


class BridgeProcess:
    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None
        self._next_id = 0
        self._stderr: deque[str] = deque(maxlen=20)
        self._stderr_task: asyncio.Task[None] | None = None

    async def start(self, snapshot: dict[str, Any] | None) -> dict[str, Any]:
        try:
            self._process = await asyncio.create_subprocess_exec(
                "node",
                str(BRIDGE_SCRIPT),
                cwd=str(ROOT),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise BridgeError(f"cannot start Node room rule process: {exc}") from exc
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        return await self.request("init", snapshot=snapshot)

    async def _drain_stderr(self) -> None:
        process = self._process
        if not process or not process.stderr:
            return
        while line := await process.stderr.readline():
            self._stderr.append(line.decode("utf-8", "replace").rstrip())

    async def request(self, op: str, **payload: Any) -> dict[str, Any]:
        process = self._process
        if not process or not process.stdin or not process.stdout:
            raise BridgeError("room rule process is not running")
        if process.returncode is not None:
            detail = "; ".join(self._stderr)
            raise BridgeError(f"room rule process exited ({process.returncode}): {detail}")
        self._next_id += 1
        request_id = self._next_id
        data = json.dumps(
            {"id": request_id, "op": op, **payload},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        process.stdin.write((data + "\n").encode("utf-8"))
        await process.stdin.drain()
        try:
            raw = await asyncio.wait_for(
                process.stdout.readline(), timeout=BRIDGE_TIMEOUT_SECONDS
            )
        except TimeoutError as exc:
            raise BridgeError(f"room rule process timed out during {op}") from exc
        if not raw:
            detail = "; ".join(self._stderr)
            raise BridgeError(f"room rule process closed its output: {detail}")
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BridgeError("room rule process returned invalid JSON") from exc
        if result.get("id") != request_id:
            raise BridgeError("room rule process returned an out-of-order response")
        if not result.get("ok"):
            raise BridgeError(result.get("error") or "room rule process failed")
        return result

    async def close(self) -> None:
        process, self._process = self._process, None
        if process and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                process.kill()
                await process.wait()
        if self._stderr_task:
            self._stderr_task.cancel()
            await asyncio.gather(self._stderr_task, return_exceptions=True)
            self._stderr_task = None


class RoomSession:
    def __init__(self, code: str, store: RoomStore):
        self.code = code
        self.store = store
        self.connections: dict[str, WebSocket] = {}
        self.bridge = BridgeProcess()
        self.lock = asyncio.Lock()
        self.snapshot = store.load(code)
        self.status: dict[str, Any] = {}
        self.ai_task: asyncio.Task[None] | None = None
        self.closed = False

    async def start(self) -> None:
        result = await self.bridge.start(self.snapshot)
        self.status = result["status"]

    async def add(self, websocket: WebSocket) -> str:
        async with self.lock:
            if self.closed:
                raise BridgeError("room session is closing")
            if len(self.connections) >= MAX_CONNECTIONS:
                raise BridgeError("room full")
            conn_id = str(uuid.uuid4())
            self.connections[conn_id] = websocket
            return conn_id

    async def message(self, conn_id: str, message: dict[str, Any]) -> None:
        msg_type = message.get("t")
        persist = msg_type in MUTATING_MESSAGES
        async with self.lock:
            result = await self._request(
                "message",
                connId=conn_id,
                message=message,
                now=int(time.time() * 1000),
                persist=persist,
            )
            await self._apply_result(result, persist)
            self._schedule_ai()

    async def remove(self, conn_id: str) -> bool:
        async with self.lock:
            self.connections.pop(conn_id, None)
            if self.closed:
                return not self.connections
            result = await self._request(
                "leave",
                connId=conn_id,
                now=int(time.time() * 1000),
                persist=True,
            )
            await self._apply_result(result, True)
            self._schedule_ai()
            return not self.connections

    async def _request(self, op: str, **payload: Any) -> dict[str, Any]:
        try:
            return await self.bridge.request(op, **payload)
        except BridgeError:
            await self.bridge.close()
            self.bridge = BridgeProcess()
            result = await self.bridge.start(self.snapshot)
            self.status = result["status"]
            # Snapshots intentionally contain no live connId mapping. Force every
            # browser to reconnect and present its stable token after a bridge crash.
            for websocket in list(self.connections.values()):
                try:
                    await websocket.close(code=1011)
                except Exception:
                    pass
            raise

    async def _apply_result(self, result: dict[str, Any], persist: bool) -> None:
        self.status = result["status"]
        if persist:
            self.snapshot = result["snapshot"]
            self.store.save(self.code, self.snapshot)
        for item in result.get("outbound", []):
            websocket = self.connections.get(item.get("connId"))
            if websocket:
                try:
                    await websocket.send_json(item["msg"])
                except Exception:
                    pass

    def _schedule_ai(self) -> None:
        should_run = bool(
            self.status.get("aiPending")
            and self.status.get("humansConnected")
            and self.connections
        )
        if should_run and (not self.ai_task or self.ai_task.done()):
            self.ai_task = asyncio.create_task(self._run_ai())
        elif not should_run and self.ai_task and self.ai_task is not asyncio.current_task():
            self.ai_task.cancel()
            self.ai_task = None

    async def _run_ai(self) -> None:
        attempt = 0
        try:
            while not self.closed:
                turn_started = int(self.status.get("turnStartedAt") or 0) / 1000
                delay = max(0.0, turn_started + AI_TURN_DELAY_SECONDS - time.time())
                await asyncio.sleep(delay)
                async with self.lock:
                    if not (
                        self.status.get("aiPending")
                        and self.status.get("humansConnected")
                        and self.connections
                    ):
                        return
                    try:
                        result = await self._request(
                            "stepAI",
                            attempt=attempt,
                            now=int(time.time() * 1000),
                            persist=True,
                        )
                    except BridgeError:
                        attempt += 1
                        continue
                    await self._apply_result(result, True)
                    attempt = 0
                    if not (
                        self.status.get("aiPending")
                        and self.status.get("humansConnected")
                        and self.connections
                    ):
                        return
        finally:
            if self.ai_task is asyncio.current_task():
                self.ai_task = None

    async def close(self) -> None:
        async with self.lock:
            self.closed = True
            if self.ai_task and self.ai_task is not asyncio.current_task():
                self.ai_task.cancel()
                await asyncio.gather(self.ai_task, return_exceptions=True)
                self.ai_task = None
            await self.bridge.close()


class RoomManager:
    def __init__(self, db_path: Path):
        self.store = RoomStore(db_path)
        self.sessions: dict[str, RoomSession] = {}
        self.lock = asyncio.Lock()

    async def connect(self, code: str, websocket: WebSocket) -> tuple[RoomSession, str]:
        async with self.lock:
            session = self.sessions.get(code)
            if not session:
                session = RoomSession(code, self.store)
                try:
                    await session.start()
                except Exception:
                    await session.close()
                    raise
                self.sessions[code] = session
            conn_id = await session.add(websocket)
            return session, conn_id

    async def disconnect(self, code: str, session: RoomSession, conn_id: str) -> None:
        empty = await session.remove(conn_id)
        if not empty:
            return
        async with self.lock:
            if self.sessions.get(code) is session and not session.connections:
                self.sessions.pop(code, None)
                await session.close()

    async def close(self) -> None:
        async with self.lock:
            sessions = list(self.sessions.values())
            self.sessions.clear()
        await asyncio.gather(*(session.close() for session in sessions))
        self.store.close()


def create_app(db_path: Path | None = None) -> FastAPI:
    database = db_path or Path(
        os.environ.get("POKEMON_SPLENDOR_DB", ROOT / "var" / "rooms.sqlite3")
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.rooms = RoomManager(database)
        yield
        await app.state.rooms.close()

    app = FastAPI(title="Pokemon Splendor", lifespan=lifespan)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/room/{raw_code}/ws")
    async def room_socket(websocket: WebSocket, raw_code: str) -> None:
        if not ROOM_CODE.fullmatch(raw_code):
            await websocket.close(code=1008)
            return
        code = raw_code.upper()
        await websocket.accept()
        manager: RoomManager = websocket.app.state.rooms
        try:
            session, conn_id = await manager.connect(code, websocket)
        except BridgeError as exc:
            await websocket.send_json({"t": "reject", "reason": str(exc)})
            await websocket.close(code=1013)
            return
        try:
            while True:
                raw = await websocket.receive_text()
                if len(raw.encode("utf-8")) > MAX_MESSAGE_BYTES:
                    continue
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(message, dict) or not isinstance(message.get("t"), str):
                    continue
                if message["t"] == "ping":
                    await websocket.send_json({"t": "pong"})
                    continue
                try:
                    await session.message(conn_id, message)
                except BridgeError:
                    try:
                        await websocket.send_json(
                            {"t": "reject", "reason": "房间服务暂时不可用，请重试"}
                        )
                    except Exception:
                        pass
                    return
        except WebSocketDisconnect:
            pass
        finally:
            await manager.disconnect(code, session, conn_id)

    app.mount("/assets", StaticFiles(directory=ROOT / "assets"), name="assets")
    app.mount("/css", StaticFiles(directory=ROOT / "css"), name="css")
    app.mount("/data", StaticFiles(directory=ROOT / "data"), name="data")
    app.mount("/js", StaticFiles(directory=ROOT / "js"), name="js")

    @app.get("/")
    @app.get("/index.html")
    async def index() -> FileResponse:
        return FileResponse(ROOT / "index.html")

    def static_file(path: Path):
        async def serve() -> FileResponse:
            return FileResponse(path)

        return serve

    for filename in (
        "manifest.json",
        "sw.js",
        "icon-192.png",
        "icon-512.png",
        "apple-touch-icon.png",
    ):
        app.add_api_route(
            f"/{filename}",
            static_file(ROOT / filename),
            methods=["GET"],
            name=f"static-{filename}",
        )

    return app


app = create_app()
