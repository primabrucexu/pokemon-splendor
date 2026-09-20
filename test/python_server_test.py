import asyncio
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import server.app as server_app
from server.app import BridgeProcess, RoomManager, RoomStore


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages = []

    async def send_json(self, message) -> None:
        self.messages.append(message)


async def wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for the Python room scheduler")
        await asyncio.sleep(0.01)


async def main() -> None:
    bridge = BridgeProcess()
    try:
        await bridge.start(None)
        first = await bridge.request(
            "message",
            connId="a",
            message={"t": "join", "name": "小明", "token": "token-a"},
            now=1000,
            persist=True,
        )
        welcome = [x["msg"] for x in first["outbound"] if x["msg"]["t"] == "welcome"][-1]
        assert welcome["seat"] == 0 and welcome["host"] is True

        second = await bridge.request(
            "message",
            connId="b",
            message={"t": "join", "name": "小霞", "token": "token-b"},
            now=1001,
            persist=True,
        )
        started = await bridge.request(
            "message",
            connId="a",
            message={"t": "start", "opts": {}},
            now=1002,
            persist=True,
        )
        assert sum(x["msg"]["t"] == "state" for x in started["outbound"]) == 2
        snapshot = started["snapshot"]
    finally:
        await bridge.close()

    restored = BridgeProcess()
    try:
        await restored.start(snapshot)
        result = await restored.request(
            "message",
            connId="a2",
            message={"t": "join", "name": "小明", "token": "token-a"},
            now=2000,
            persist=True,
        )
        messages = [x["msg"] for x in result["outbound"] if x["connId"] == "a2"]
        welcome = next(x for x in messages if x["t"] == "welcome")
        assert welcome["seat"] == 0 and welcome["host"] is True
        assert any(x["t"] == "state" for x in messages)
    finally:
        await restored.close()

    with tempfile.TemporaryDirectory(dir=ROOT / "test") as directory:
        store = RoomStore(Path(directory) / "rooms.sqlite3")
        try:
            store.save("ABC123", snapshot)
            assert store.load("ABC123") == snapshot
        finally:
            store.close()

    # Exercise Python's room lifecycle and AI scheduler, not only the JS bridge.
    server_app.AI_TURN_DELAY_SECONDS = 0.01
    with tempfile.TemporaryDirectory(dir=ROOT / "test") as directory:
        manager = RoomManager(Path(directory) / "rooms.sqlite3")
        host = FakeWebSocket()
        session, host_id = await manager.connect("VISIBILITY", host)
        await session.message(
            host_id, {"t": "join", "name": "小明", "token": "host-token"}
        )
        guest = FakeWebSocket()
        guest_session, guest_id = await manager.connect("VISIBILITY", guest)
        assert guest_session is session
        await guest_session.message(
            guest_id, {"t": "join", "name": "小霞", "token": "guest-token"}
        )
        assert any(
            message.get("t") == "roster" and len(message.get("players", [])) == 2
            for message in host.messages
        )
        assert any(
            message.get("t") == "roster" and len(message.get("players", [])) == 2
            for message in guest.messages
        )
        await manager.disconnect("VISIBILITY", guest_session, guest_id)
        await manager.disconnect("VISIBILITY", session, host_id)
        await manager.close()

    with tempfile.TemporaryDirectory(dir=ROOT / "test") as directory:
        manager = RoomManager(Path(directory) / "rooms.sqlite3")
        host = FakeWebSocket()
        session, conn_id = await manager.connect("PYTEST", host)
        await session.message(conn_id, {"t": "join", "name": "小明", "token": "host-token"})
        await session.message(conn_id, {"t": "addAI", "level": "easy"})
        await session.message(conn_id, {"t": "start", "opts": {}})
        await session.message(
            conn_id,
            {"t": "action", "action": {"type": "take", "colors": ["red", "blue", "black"]}},
        )
        await session.message(conn_id, {"t": "action", "action": {"type": "endTurn"}})
        await wait_until(
            lambda: any(
                message.get("t") == "state" and message["state"].get("turn") == 0
                for message in host.messages[-3:]
            )
        )
        saved_seq = session.snapshot["seq"]
        await manager.disconnect("PYTEST", session, conn_id)

        reconnected = FakeWebSocket()
        restored_session, restored_id = await manager.connect("PYTEST", reconnected)
        await restored_session.message(
            restored_id,
            {"t": "join", "name": "小明", "token": "host-token"},
        )
        assert restored_session.snapshot["seq"] == saved_seq
        assert any(message.get("t") == "state" for message in reconnected.messages)
        await manager.disconnect("PYTEST", restored_session, restored_id)
        await manager.close()

    print("PASS Python bridge, SQLite recovery, room lifecycle, and AI scheduler")


if __name__ == "__main__":
    asyncio.run(main())
