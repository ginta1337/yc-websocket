#!/usr/bin/env python3
import argparse
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

from aiohttp import WSMsgType, web


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("ws-sim-server")


@dataclass(frozen=True)
class FrameSpec:
    label: str
    size: int
    response_label: str
    response_size: int


REQUEST_FLOW: List[FrameSpec] = [
    FrameSpec("A", 1605, "F", 4012),
    FrameSpec("B", 80, "G", 543),
    FrameSpec("C", 122, "H", 44),
    FrameSpec("D", 31, "I", 1622),
    FrameSpec("E", 72, "J", 63),
]
ERROR_PAYLOAD = b"X" * 24


def build_payload(label: str, size: int) -> bytes:
    return label.encode("ascii") * size


EXPECTED_REQUESTS: Dict[Tuple[str, int], FrameSpec] = {
    (spec.label, spec.size): spec for spec in REQUEST_FLOW
}


def describe_payload(payload: bytes) -> Tuple[str, int]:
    if not payload:
        return "-", 0
    label = chr(payload[0]) if 32 <= payload[0] <= 126 else f"0x{payload[0]:02x}"
    return label, len(payload)


async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    peer = request.remote or "-"
    LOGGER.info("client connected peer=%s", peer)

    expected_index = 0
    try:
        async for message in ws:
            if message.type != WSMsgType.BINARY:
                LOGGER.warning("unexpected websocket message type=%s peer=%s", message.type, peer)
                continue

            payload = message.data
            label, size = describe_payload(payload)

            expected = REQUEST_FLOW[expected_index] if expected_index < len(REQUEST_FLOW) else None
            LOGGER.info(
                "received frame peer=%s actual=%s size=%s expected=%s",
                peer,
                label,
                size,
                expected.label if expected else "-",
            )

            if expected is None or (label, size) != (expected.label, expected.size):
                LOGGER.warning(
                    "SERVER DISORDER detected peer=%s actual=%s/%s expected=%s/%s sending_error_size=%s",
                    peer,
                    label,
                    size,
                    expected.label if expected else "-",
                    expected.size if expected else "-",
                    len(ERROR_PAYLOAD),
                )
                await ws.send_bytes(ERROR_PAYLOAD)
                await ws.close()
                break

            response = build_payload(expected.response_label, expected.response_size)
            LOGGER.info(
                "sending response peer=%s label=%s size=%s",
                peer,
                expected.response_label,
                expected.response_size,
            )
            await ws.send_bytes(response)
            expected_index += 1

            if expected_index == len(REQUEST_FLOW):
                LOGGER.info("protocol completed peer=%s", peer)
                await asyncio.sleep(0.05)
                await ws.close()
                break
    finally:
        LOGGER.info("client disconnected peer=%s", peer)

    return ws


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ws", handle_ws)
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="WebSocket server that validates the simulated bridge ordering.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    web.run_app(create_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
