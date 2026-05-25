#!/usr/bin/env python3
import argparse
import asyncio
import heapq
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

from aiohttp import ClientSession, WSMsgType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("ws-sim-client")


@dataclass(frozen=True)
class IncomingFrame:
    arrival_seq: int
    message_id: str
    delay_from_previous: float
    label: str
    size: int


@dataclass(order=True)
class PendingFrame:
    sort_key: str
    arrival_seq: int
    frame: IncomingFrame
    queued_at: float


@dataclass(frozen=True)
class ExpectedResponse:
    label: str
    size: int


INCOMING_FRAMES: List[IncomingFrame] = [
    IncomingFrame(1, "id-1", 0.181, "A", 1605),
    IncomingFrame(2, "id-2", 0.394, "B", 80),
    IncomingFrame(3, "id-3", 0.000, "C", 122),
    IncomingFrame(4, "id-4", 0.000, "D", 31),
    IncomingFrame(5, "id-5", 0.485, "E", 72),
]

SORT_MODE_RESPONSES = [
    ExpectedResponse("F", 4012),
    ExpectedResponse("G", 543),
    ExpectedResponse("H", 44),
    ExpectedResponse("I", 1622),
    ExpectedResponse("J", 63),
]
ARRIVAL_MODE_RESPONSES = [
    ExpectedResponse("F", 4012),
    ExpectedResponse("G", 543),
    ExpectedResponse("X", 24),
]


def build_payload(label: str, size: int) -> bytes:
    return label.encode("ascii") * size


def describe_payload(payload: bytes) -> Tuple[str, int]:
    if not payload:
        return "-", 0
    label = chr(payload[0]) if 32 <= payload[0] <= 126 else f"0x{payload[0]:02x}"
    return label, len(payload)


class BridgeSimulatorClient:
    def __init__(self, url: str, mode: str, reorder_window: float) -> None:
        self._url = url
        self._mode = mode
        self._reorder_window = reorder_window
        self._pending: List[PendingFrame] = []
        self._pending_notify = asyncio.Event()
        self._done_arrivals = False
        self._send_history: List[IncomingFrame] = []
        self._expected_responses = SORT_MODE_RESPONSES if mode == "sort" else ARRIVAL_MODE_RESPONSES
        self._response_index = 0

    async def run(self) -> None:
        async with ClientSession() as session:
            async with session.ws_connect(self._url) as ws:
                sender = asyncio.create_task(self._sender(ws))
                receiver = asyncio.create_task(self._receiver(ws))
                try:
                    await self._simulate_arrivals()
                    self._done_arrivals = True
                    self._pending_notify.set()
                    await sender
                    await receiver
                finally:
                    sender.cancel()
                    receiver.cancel()

    async def _simulate_arrivals(self) -> None:
        previous_arrival: Optional[IncomingFrame] = None
        for frame in INCOMING_FRAMES:
            await asyncio.sleep(frame.delay_from_previous)
            LOGGER.info(
                "simulated Yandex arrival arrival_seq=%s message_id=%s label=%s size=%s",
                frame.arrival_seq,
                frame.message_id,
                frame.label,
                frame.size,
            )
            queued_at = asyncio.get_running_loop().time()
            pending = PendingFrame(
                sort_key=frame.message_id if self._mode == "sort" else f"arrival-{frame.arrival_seq:020d}",
                arrival_seq=frame.arrival_seq,
                frame=frame,
                queued_at=queued_at,
            )
            heapq.heappush(self._pending, pending)

            by_arrival = self._pending_by_arrival()
            by_sort = self._pending_by_sort()
            LOGGER.info(
                "queued frame arrival_seq=%s message_id=%s pending=%s mode=%s",
                frame.arrival_seq,
                frame.message_id,
                len(self._pending),
                self._mode,
            )
            if previous_arrival is not None and len(by_arrival) > 1:
                if [item.arrival_seq for item in by_arrival] != [item.arrival_seq for item in by_sort]:
                    LOGGER.warning(
                        "CLIENT pending order differs arrival_order=[%s] send_order=[%s]",
                        self._format_pending(by_arrival),
                        self._format_pending(by_sort),
                    )
                else:
                    LOGGER.info(
                        "CLIENT pending order matches arrival order=[%s]",
                        self._format_pending(by_sort),
                    )
            self._pending_notify.set()
            previous_arrival = frame

    async def _sender(self, ws) -> None:
        while True:
            if not self._pending:
                if self._done_arrivals:
                    return
                self._pending_notify.clear()
                await self._pending_notify.wait()
                continue

            next_frame = self._pending[0]
            send_at = next_frame.queued_at + self._reorder_window
            delay = send_at - asyncio.get_running_loop().time()
            if delay > 0:
                await asyncio.sleep(delay)
                continue

            by_arrival = self._pending_by_arrival()
            by_sort = self._pending_by_sort()
            oldest = by_arrival[0]
            if oldest.arrival_seq != next_frame.arrival_seq:
                LOGGER.warning(
                    "CLIENT REORDER DECISION sending arrival_seq=%s id=%s before oldest arrival_seq=%s id=%s arrival_order=[%s] send_order=[%s]",
                    next_frame.arrival_seq,
                    next_frame.frame.message_id,
                    oldest.arrival_seq,
                    oldest.frame.message_id,
                    self._format_pending(by_arrival),
                    self._format_pending(by_sort),
                )

            pending = heapq.heappop(self._pending)
            payload = build_payload(pending.frame.label, pending.frame.size)
            LOGGER.info(
                "CLIENT sending label=%s size=%s arrival_seq=%s message_id=%s",
                pending.frame.label,
                pending.frame.size,
                pending.arrival_seq,
                pending.frame.message_id,
            )
            await ws.send_bytes(payload)
            self._send_history.append(pending.frame)

    async def _receiver(self, ws) -> None:
        async for message in ws:
            if message.type != WSMsgType.BINARY:
                LOGGER.warning("CLIENT received non-binary websocket message type=%s", message.type)
                continue

            label, size = describe_payload(message.data)
            expected = self._expected_responses[self._response_index] if self._response_index < len(self._expected_responses) else None
            LOGGER.info(
                "CLIENT received response label=%s size=%s expected=%s/%s",
                label,
                size,
                expected.label if expected else "-",
                expected.size if expected else "-",
            )

            if expected is None or (label, size) != (expected.label, expected.size):
                LOGGER.warning(
                    "CLIENT RESPONSE DISORDER label=%s size=%s expected=%s/%s",
                    label,
                    size,
                    expected.label if expected else "-",
                    expected.size if expected else "-",
                )
            else:
                self._response_index += 1

        LOGGER.info("CLIENT websocket closed")

    def _pending_by_arrival(self) -> List[PendingFrame]:
        return sorted(self._pending, key=lambda item: item.arrival_seq)

    def _pending_by_sort(self) -> List[PendingFrame]:
        return sorted(self._pending)

    @staticmethod
    def _format_pending(items: List[PendingFrame]) -> str:
        return ", ".join(
            f"seq={item.arrival_seq}:id={item.frame.message_id}:label={item.frame.label}:size={item.frame.size}"
            for item in items
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="WebSocket client that simulates the bridge reorder logs.",
    )
    parser.add_argument("--url", default="http://127.0.0.1:8765/ws")
    parser.add_argument(
        "--mode",
        choices=["sort", "arrival"],
        default="sort",
        help="sort reproduces the lexicographic reorder from the sample; arrival disables it.",
    )
    parser.add_argument("--reorder-window", type=float, default=0.1)
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    client = BridgeSimulatorClient(
        url=args.url,
        mode=args.mode,
        reorder_window=args.reorder_window,
    )
    await client.run()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
