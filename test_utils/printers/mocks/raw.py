import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

_logger = logging.getLogger(__name__)


@dataclass
class ReceivedJob:
	"""A job received by the raw socket mock printer."""

	timestamp: datetime
	peer: tuple[str, int] | None
	size: int
	data: bytes


class RawPrinterMock:
	"""Simulates a raw socket printer (port 9100 style)."""

	def __init__(
		self,
		host: str = "127.0.0.1",
		port: int = 0,
		save_dir: Path | None = None,
		delay: float = 0.0,
	):
		self.host = host
		self.port = port
		self.save_dir = save_dir
		self.delay = delay

		self.jobs_received: list[ReceivedJob] = []
		self._server: asyncio.Server | None = None
		self._actual_port: int | None = None

	@property
	def actual_port(self) -> int:
		if self._actual_port is not None:
			return self._actual_port
		return self.port

	@property
	def address(self) -> str:
		return f"{self.host}:{self.actual_port}"

	@property
	def device_uri(self) -> str:
		return f"socket://{self.host}:{self.actual_port}"

	@property
	def uri(self) -> str:
		return self.device_uri

	@property
	def received_payloads(self) -> list[bytes]:
		return [job.data for job in self.jobs_received]

	async def start(self) -> None:
		self._server = await asyncio.start_server(
			self.handle_connection,
			self.host,
			self.port,
		)
		sockets = self._server.sockets
		if sockets:
			self._actual_port = sockets[0].getsockname()[1]
		_logger.info("Raw printer mock listening on %s", self.device_uri)

	async def stop(self) -> None:
		if self._server:
			self._server.close()
			await self._server.wait_closed()
			self._server = None
			_logger.info("Raw printer mock stopped")

	async def __aenter__(self) -> "RawPrinterMock":
		await self.start()
		return self

	async def __aexit__(self, *args) -> None:
		await self.stop()

	async def handle_connection(
		self,
		reader: asyncio.StreamReader,
		writer: asyncio.StreamWriter,
	) -> None:
		peer = writer.get_extra_info("peername")
		try:
			chunks = []
			while True:
				chunk = await reader.read(8192)
				if not chunk:
					break
				chunks.append(chunk)

			data = b"".join(chunks)
			if not data:
				return

			if self.delay > 0:
				await asyncio.sleep(self.delay)

			timestamp = datetime.now()
			job = ReceivedJob(
				timestamp=timestamp,
				peer=peer,
				size=len(data),
				data=data,
			)
			self.jobs_received.append(job)
			_logger.info("Received raw job: %s bytes from %s", len(data), peer)

			if self.save_dir:
				self.save_dir.mkdir(parents=True, exist_ok=True)
				filename = f"job_{timestamp.isoformat().replace(':', '-')}.raw"
				(self.save_dir / filename).write_bytes(data)
		except Exception as exc:
			_logger.error("Error handling raw print connection: %s", exc)
		finally:
			if not writer.is_closing():
				writer.close()
				await writer.wait_closed()

	def clear(self) -> None:
		self.jobs_received.clear()

	@property
	def job_count(self) -> int:
		return len(self.jobs_received)

	@property
	def last_job(self) -> ReceivedJob | None:
		return self.jobs_received[-1] if self.jobs_received else None

	@property
	def total_bytes(self) -> int:
		return sum(job.size for job in self.jobs_received)

	def last_text(self) -> str:
		payload = self.received_payloads[-1] if self.received_payloads else b""
		return payload.decode("utf-8", errors="replace")

	def wait_for_payload(self, timeout: float = 5.0) -> bytes:
		deadline = time.time() + timeout
		while time.time() < deadline:
			if self.received_payloads:
				return self.received_payloads[-1]
			time.sleep(0.1)
		return b""

	async def wait_for_jobs(self, count: int, timeout: float = 5.0) -> bool:
		deadline = asyncio.get_event_loop().time() + timeout
		while self.job_count < count:
			if asyncio.get_event_loop().time() > deadline:
				return False
			await asyncio.sleep(0.05)
		return True


@asynccontextmanager
async def raw_printer(**kwargs):
	mock = RawPrinterMock(**kwargs)
	await mock.start()
	try:
		yield mock
	finally:
		await mock.stop()


MockRawPrinter = RawPrinterMock
