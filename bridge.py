#!/usr/bin/env python3
import asyncio
import heapq
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from aiohttp import ClientSession, ClientWebSocketResponse, WSMsgType, web
from yandexcloud import SDK
from yandex.cloud.serverless.apigateway.websocket.v1.connection_service_pb2 import (
    DisconnectRequest,
    SendToConnectionRequest,
)
from yandex.cloud.serverless.apigateway.websocket.v1.connection_service_pb2_grpc import (
    ConnectionServiceStub,
)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("yc-http-ws-bridge")

def _enum_value(name: str, default: int) -> int:
    value = getattr(SendToConnectionRequest, name, None)
    if value is not None:
        return int(value)
    return default


TEXT_DATA_TYPE = _enum_value("TEXT", 2)
BINARY_DATA_TYPE = _enum_value("BINARY", 1)


@dataclass
class PendingMessage:
    message_id: str
    sort_key: int
    received_at: float
    content_type: str
    payload: bytes


@dataclass
class BridgeSession:
    connection_id: str
    upstream_ws: ClientWebSocketResponse
    reader_task: Optional[asyncio.Task]
    writer_task: Optional[asyncio.Task]
    pending_messages: List[Tuple[int, int, str, PendingMessage]]
    pending_notify: asyncio.Event


class YcConnectionApi:
    def __init__(self) -> None:
        iam_token = os.getenv("YC_IAM_TOKEN")
        if iam_token:
            self._sdk = SDK(iam_token=iam_token)
        else:
            self._sdk = SDK()

        self._client = self._sdk.client(ConnectionServiceStub)

    async def send_text(self, connection_id: str, text: str) -> None:
        await self._send(connection_id, text.encode("utf-8"), TEXT_DATA_TYPE)

    async def send_binary(self, connection_id: str, data: bytes) -> None:
        await self._send(connection_id, data, BINARY_DATA_TYPE)

    async def _send(self, connection_id: str, data: bytes, data_type: int) -> None:
        request = SendToConnectionRequest(
            connection_id=connection_id,
            data=data,
            type=data_type,
        )
        await asyncio.to_thread(self._client.Send, request)

    async def disconnect(self, connection_id: str) -> None:
        request = DisconnectRequest(connection_id=connection_id)
        await asyncio.to_thread(self._client.Disconnect, request)


class WebSocketBridge:
    def __init__(self) -> None:
        self._upstream_url = os.environ["UPSTREAM_WS_URL"]
        self._connect_timeout = float(os.getenv("UPSTREAM_CONNECT_TIMEOUT", "5"))
        self._reorder_window = float(os.getenv("MESSAGE_REORDER_WINDOW", "0.05"))
        self._client_session: Optional[ClientSession] = None
        self._yc = YcConnectionApi()
        self._sessions: Dict[str, BridgeSession] = {}
        self._sessions_lock = asyncio.Lock()
        self._process_t0 = time.perf_counter()

    @staticmethod
    def _parse_message_id_base36(message_id: [str]) -> Optional[int]:
        stripped = message_id.strip()
        try:
            return int(stripped, 36)
        except ValueError:
            LOGGER.error("FAIL convert message id to int %s", message_id)
            return None

    def _format_pending_message(self, message: PendingMessage) -> str:
        return f"at: {message.received_at - self._process_t0:.4f}s id: {message.message_id} size: {len(message.payload)}"

    async def _pop_session(self, connection_id: str) -> Optional[BridgeSession]:
        async with self._sessions_lock:
            return self._sessions.pop(connection_id, None)

    async def _ensure_session(self, connection_id: str, request: web.Request) -> BridgeSession:
        async with self._sessions_lock:
            session = self._sessions.get(connection_id)
        if session:
            return session

        LOGGER.warning("session %s not found, reconnecting upstream", connection_id)
        await self._handle_connect(request, connection_id)

        async with self._sessions_lock:
            session = self._sessions.get(connection_id)
            if not session:
                raise web.HTTPBadGateway(text="failed to create upstream websocket")
            return session

    async def start(self) -> None:
        if self._client_session is None:
            self._client_session = ClientSession()

    async def close(self) -> None:
        async with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()

        for session in sessions:
            if session.reader_task:
                session.reader_task.cancel()
            if session.writer_task:
                session.writer_task.cancel()
            await session.upstream_ws.close()

        if self._client_session is not None:
            await self._client_session.close()
            self._client_session = None

    async def handle_event(self, request: web.Request) -> web.Response:
        event_type = request.headers.get("X-Yc-Apigateway-Websocket-Event-Type", "").upper()
        connection_id = request.headers.get("X-Yc-Apigateway-Websocket-Connection-Id")

        if not connection_id:
            LOGGER.error("received request with missing connection id!")
            return web.json_response({"error": "missing connection id"}, status=400)

        if event_type == "CONNECT":
            await self._handle_connect(request, connection_id)
            return web.Response(status=204)

        if event_type == "MESSAGE":
            payload = await request.read()
            await self._handle_message(request, connection_id, payload)
            return web.Response(status=204)

        if event_type == "DISCONNECT":
            await self._handle_disconnect(connection_id)
            return web.Response(status=204)

        LOGGER.error("received Message with event type!")
        return web.json_response({"error": f"unsupported event type: {event_type}"}, status=400)

    async def _handle_connect(self, request: web.Request, connection_id: str) -> None:
        LOGGER.info("received CONNECT event for %s", connection_id)
        if self._client_session is None:
            raise web.HTTPServiceUnavailable(text="bridge is not ready")

        async with self._sessions_lock:
            existing = self._sessions.get(connection_id)
            if existing:
                LOGGER.info("connect event for existing connection %s", connection_id)
                return

        headers = {}
        if request.headers.get("Authorization"):
            headers["Authorization"] = request.headers["Authorization"]
        if request.headers.get("Sec-WebSocket-Protocol"):
            headers["Sec-WebSocket-Protocol"] = request.headers["Sec-WebSocket-Protocol"]

        LOGGER.info("opening upstream websocket for %s", connection_id)
        upstream_ws = await self._client_session.ws_connect(
            self._upstream_url,
            headers=headers or None,
            timeout=self._connect_timeout,
        )

        session = BridgeSession(
            connection_id=connection_id,
            upstream_ws=upstream_ws,
            reader_task=None,
            writer_task=None,
            pending_messages=[],
            pending_notify=asyncio.Event(),
        )

        async with self._sessions_lock:
            self._sessions[connection_id] = session

        session.reader_task = asyncio.create_task(self._relay_upstream(session))
        session.writer_task = asyncio.create_task(self._drain_pending_messages(session))

    async def _handle_disconnect(self, connection_id: str) -> None:
        LOGGER.info("received DISCONNECT event for %s", connection_id)
        session = await self._pop_session(connection_id)
        if not session:
            return

        LOGGER.debug("closing upstream websocket for %s", connection_id)
        if session.reader_task:
            session.reader_task.cancel()
        if session.writer_task:
            session.writer_task.cancel()
        await session.upstream_ws.close()

    async def _handle_message(self, request: web.Request, connection_id: str, payload: bytes) -> None:
        content_type = request.headers.get("Content-Type")
        message_id = request.headers.get("X-Yc-Apigateway-Websocket-Message-Id")

        pending = PendingMessage(
            message_id=message_id,
            sort_key=self._parse_message_id_base36(message_id),
            received_at=asyncio.get_running_loop().time(),
            content_type=content_type,
            payload=payload,
        )
        LOGGER.debug(
            "received MESSAGE event for %s %s",
            connection_id,
            self._format_pending_message(pending),
        )
        if not content_type or not message_id:
            LOGGER.error("skipping MESSAGE for %s: missing required headers", connection_id)
            return

        session = await self._ensure_session(connection_id, request)

        heapq.heappush(
            session.pending_messages,
            (-pending.sort_key, pending),
        )

        LOGGER.info(
            "queued message for %s id: %s pending=%d",
            connection_id,
            pending.message_id,
            len(session.pending_messages),
        )

        session.pending_notify.set()

    async def _relay_upstream(self, session: BridgeSession) -> None:
        connection_id = session.connection_id
        LOGGER.info("Upstream relay started for %s", connection_id)
        try:
            async for message in session.upstream_ws:
                if message.type == WSMsgType.TEXT:
                    LOGGER.debug(
                        "forwarding Upstream text message for %s size=%s",
                        connection_id,
                        len(message.data.encode("utf-8")),
                    )
                    await self._yc.send_text(connection_id, message.data)
                elif message.type == WSMsgType.BINARY:
                    LOGGER.debug(
                        "forwarding Upstream binary message for %s size=%s",
                        connection_id,
                        len(message.data),
                    )
                    await self._yc.send_binary(connection_id, message.data)
                elif message.type == WSMsgType.ERROR:
                    LOGGER.debug(f"forwarding upstream status message for {connection_id}")
                    raise RuntimeError("upstream websocket reported an error")
                elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED}:
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("upstream relay failed for %s", connection_id)
        finally:
            LOGGER.info("upstream relay finished for %s", connection_id)
            removed = await self._pop_session(connection_id)
            if removed is session:
                if session.writer_task:
                    session.writer_task.cancel()
                try:
                    await self._yc.disconnect(connection_id)
                except Exception:
                    LOGGER.error("failed to disconnect gateway connection %s", connection_id)

    async def _drain_pending_messages(self, session: BridgeSession) -> None:
        connection_id = session.connection_id
        LOGGER.info("message reorder worker started for %s", connection_id)
        try:
            while True:
                if not session.pending_messages:
                    session.pending_notify.clear()
                    await session.pending_notify.wait()
                    continue

                *_, pending = session.pending_messages[0]
                send_at = pending.received_at + self._reorder_window
                delay = send_at - asyncio.get_running_loop().time()
                if delay > 0:
                    await asyncio.sleep(delay)
                    continue

                *_, pending = heapq.heappop(session.pending_messages)
                if pending.content_type.startswith("application/octet-stream"):
                    LOGGER.debug(
                        "sending to Upstream WS binary for %s %s",
                        connection_id,
                        self._format_pending_message(pending),
                    )
                    await session.upstream_ws.send_bytes(pending.payload)
                else:
                    LOGGER.debug(
                        "sending to Upstream WS text for %s %s",
                        connection_id,
                        self._format_pending_message(pending),
                    )
                    await session.upstream_ws.send_str(pending.payload.decode("utf-8"))
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("message reorder worker failed for %s", connection_id)
        finally:
            LOGGER.info("message reorder worker finished for %s", connection_id)

async def healthcheck(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


def create_app() -> web.Application:
    bridge = WebSocketBridge()
    app = web.Application()
    app["bridge"] = bridge
    app.router.add_get("/healthz", healthcheck)
    app.router.add_post("/ws", bridge.handle_event)
    app.router.add_get("/ws", bridge.handle_event)

    async def on_startup(app: web.Application) -> None:
        await app["bridge"].start()

    async def on_cleanup(app: web.Application) -> None:
        await app["bridge"].close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    web.run_app(create_app(), host=host, port=port)
