from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Any


class RedisProtocolError(RuntimeError):
    pass


def _encode_command(parts: list[str]) -> bytes:
    encoded = [f"*{len(parts)}\r\n".encode("ascii")]
    for part in parts:
        value = str(part).encode("utf-8")
        encoded.extend((f"${len(value)}\r\n".encode("ascii"), value, b"\r\n"))
    return b"".join(encoded)


class _RespConnection:
    def __init__(self, host: str, port: int, timeout_seconds: float):
        self.socket = socket.create_connection((host, port), timeout=timeout_seconds)
        self.socket.settimeout(timeout_seconds)
        self.file = self.socket.makefile("rb")

    def close(self) -> None:
        self.file.close()
        self.socket.close()

    def execute(self, parts: list[str]) -> Any:
        self.socket.sendall(_encode_command(parts))
        return self._read()

    def _read_line(self) -> bytes:
        value = self.file.readline()
        if not value.endswith(b"\r\n"):
            raise RedisProtocolError("truncated Redis response")
        return value[:-2]

    def _read(self) -> Any:
        prefix = self.file.read(1)
        if not prefix:
            raise RedisProtocolError("Redis connection closed")
        line = self._read_line()
        if prefix == b"+":
            return line.decode("utf-8")
        if prefix == b"-":
            raise RedisProtocolError(line.decode("utf-8"))
        if prefix == b":":
            return int(line)
        if prefix == b"$":
            size = int(line)
            if size == -1:
                return None
            value = self.file.read(size)
            if len(value) != size or self.file.read(2) != b"\r\n":
                raise RedisProtocolError("truncated Redis bulk string")
            return value.decode("utf-8")
        if prefix == b"*":
            count = int(line)
            if count == -1:
                return None
            return [self._read() for _ in range(count)]
        raise RedisProtocolError("unknown Redis response type")


def _field_map(values: list[Any]) -> dict[str, str]:
    if len(values) % 2:
        raise RedisProtocolError("Redis Stream field list has odd length")
    return {str(values[index]): str(values[index + 1]) for index in range(0, len(values), 2)}


@dataclass(frozen=True)
class RedisStreamMessage:
    message_id: str
    fields: dict[str, str]


class RedisStreamConsumer:
    """Small Redis Streams Consumer Group adapter using only Python stdlib.

    It intentionally exposes only the commands needed by Phase B: group create,
    pending-message reclaim, XREADGROUP and XACK. Newer Redis uses XAUTOCLAIM;
    Redis 5/6 falls back to XPENDING + XCLAIM. This keeps the AI MVP runnable
    without a second Python Redis client dependency.
    """

    def __init__(self, host: str, port: int, stream: str, group: str, consumer: str,
                 block_ms: int = 500, reclaim_idle_ms: int = 300000):
        self.host, self.port = host, port
        self.stream, self.group, self.consumer = stream, group, consumer
        self.block_ms, self.reclaim_idle_ms = block_ms, reclaim_idle_ms
        self._group_ready = False
        self._xautoclaim_available: bool | None = None

    def _execute(self, parts: list[str], timeout_seconds: float = 10) -> Any:
        connection = _RespConnection(self.host, self.port, timeout_seconds)
        try:
            return connection.execute(parts)
        finally:
            connection.close()

    def ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            self._execute(["XGROUP", "CREATE", self.stream, self.group, "0", "MKSTREAM"])
        except RedisProtocolError as exc:
            if not str(exc).startswith("BUSYGROUP"):
                raise
        self._group_ready = True

    def read_one(self) -> RedisStreamMessage | None:
        self.ensure_group()
        message = self._reclaim_one()
        if message is not None:
            return message
        reply = self._execute(
            ["XREADGROUP", "GROUP", self.group, self.consumer, "COUNT", "1", "BLOCK", str(self.block_ms),
             "STREAMS", self.stream, ">"],
            timeout_seconds=max(10, self.block_ms / 1000 + 5),
        )
        if not reply:
            return None
        if not isinstance(reply, list) or not reply or not isinstance(reply[0], list) or len(reply[0]) != 2:
            raise RedisProtocolError("invalid XREADGROUP response")
        messages = self._entries(reply[0][1])
        return messages[0] if messages else None

    def _reclaim_one(self) -> RedisStreamMessage | None:
        if self._xautoclaim_available is not False:
            try:
                claimed = self._execute(
                    ["XAUTOCLAIM", self.stream, self.group, self.consumer,
                     str(self.reclaim_idle_ms), "0-0", "COUNT", "1"]
                )
                self._xautoclaim_available = True
                if isinstance(claimed, list) and len(claimed) >= 2:
                    messages = self._entries(claimed[1])
                    return messages[0] if messages else None
            except RedisProtocolError as exc:
                if "unknown command `XAUTOCLAIM`" not in str(exc):
                    raise
                self._xautoclaim_available = False

        pending = self._execute(["XPENDING", self.stream, self.group, "-", "+", "1"])
        if not isinstance(pending, list) or not pending or not isinstance(pending[0], list):
            return None
        entry_id, _owner, idle_ms, _deliveries = pending[0]
        if int(idle_ms) < self.reclaim_idle_ms:
            return None
        claimed = self._execute(
            ["XCLAIM", self.stream, self.group, self.consumer,
             str(self.reclaim_idle_ms), str(entry_id)]
        )
        messages = self._entries(claimed)
        return messages[0] if messages else None

    def acknowledge(self, message_id: str) -> None:
        value = self._execute(["XACK", self.stream, self.group, message_id])
        if int(value) != 1:
            raise RedisProtocolError(f"XACK did not acknowledge {message_id}")

    @staticmethod
    def _entries(entries: Any) -> list[RedisStreamMessage]:
        if not isinstance(entries, list):
            return []
        result: list[RedisStreamMessage] = []
        for entry in entries:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], list):
                continue
            result.append(RedisStreamMessage(str(entry[0]), _field_map(entry[1])))
        return result
