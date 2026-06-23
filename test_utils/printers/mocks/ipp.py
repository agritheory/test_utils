import asyncio
import logging
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from test_utils.printers.ipp.codec import (
	IppDecoder,
	IppEncoder,
	IppJobState,
	IppOperation,
	IppStatus,
	IppTag,
	build_error_response,
)

_logger = logging.getLogger(__name__)


@dataclass
class IppJob:
	"""A job held by the IPP printer mock."""

	id: int
	name: str
	state: IppJobState
	document_format: str
	data: bytes
	created: datetime = field(default_factory=datetime.now)


MockIppJob = IppJob


class IppPrinterMock:
	"""Mock IPP printer server using asyncio HTTP handling."""

	DEFAULT_FORMATS = [
		"application/pdf",
		"image/pwg-raster",
		"image/urf",
		"application/octet-stream",
	]

	def __init__(
		self,
		host: str = "127.0.0.1",
		port: int = 0,
		name: str = "Mock IPP Printer",
		printer_path: str = "/ipp/print",
		save_dir: Path | None = None,
		supported_formats: list[str] | None = None,
		fail_rate: float = 0.0,
		delay: float = 0.0,
	):
		self.host = host
		self.port = port
		self.name = name
		self.printer_path = (
			printer_path if printer_path.startswith("/") else f"/{printer_path}"
		)
		self.save_dir = save_dir
		self.supported_formats = supported_formats or self.DEFAULT_FORMATS
		self.fail_rate = fail_rate
		self.delay = delay

		self.jobs: dict[int, IppJob] = {}
		self._next_job_id = 1
		self._server: asyncio.Server | None = None
		self._actual_port: int | None = None

		self.printer_state: Literal["idle", "processing", "error"] = "idle"
		self.state_reasons: list[str] = []
		self.page_count: int = 0
		self.supplies: dict[str, int] = {}

	@property
	def actual_port(self) -> int:
		if self._actual_port is not None:
			return self._actual_port
		return self.port

	@property
	def address(self) -> str:
		return f"{self.host}:{self.actual_port}"

	@property
	def uri(self) -> str:
		return f"ipp://{self.host}:{self.actual_port}{self.printer_path}"

	@property
	def device_uri(self) -> str:
		return self.uri

	async def start(self) -> None:
		self._server = await asyncio.start_server(
			self.handle_connection,
			self.host,
			self.port,
		)
		sockets = self._server.sockets
		if sockets:
			self._actual_port = sockets[0].getsockname()[1]
		_logger.info("IPP printer mock listening on %s", self.uri)

	async def stop(self) -> None:
		if self._server:
			self._server.close()
			await self._server.wait_closed()
			self._server = None
			_logger.info("IPP printer mock stopped")

	async def __aenter__(self) -> "IppPrinterMock":
		await self.start()
		return self

	async def __aexit__(self, *args) -> None:
		await self.stop()

	def set_state(
		self,
		state: Literal["idle", "processing", "error"],
		reasons: list[str] | None = None,
	) -> None:
		self.printer_state = state
		self.state_reasons = reasons or []

	def set_page_count(self, count: int) -> None:
		self.page_count = count

	def set_supplies(self, supplies: dict[str, int]) -> None:
		self.supplies = supplies

	async def handle_connection(
		self,
		reader: asyncio.StreamReader,
		writer: asyncio.StreamWriter,
	) -> None:
		try:
			request_line = await reader.readline()
			if not request_line:
				return

			headers: dict[str, str] = {}
			while True:
				line = await reader.readline()
				if line in (b"\r\n", b"\n") or not line:
					break
				if b":" in line:
					key, value = line.decode("utf-8").split(":", 1)
					headers[key.strip().lower()] = value.strip()

			content_length = int(headers.get("content-length", 0))
			body = await reader.readexactly(content_length) if content_length > 0 else b""
			response_body = await self.handle_ipp_request(body)

			response = (
				b"HTTP/1.1 200 OK\r\n"
				b"Content-Type: application/ipp\r\n"
				b"Content-Length: " + str(len(response_body)).encode() + b"\r\n"
				b"\r\n" + response_body
			)
			writer.write(response)
			await writer.drain()
		except Exception as exc:
			_logger.error("Error handling IPP connection: %s", exc)
		finally:
			writer.close()
			await writer.wait_closed()

	async def handle_ipp_request(self, data: bytes) -> bytes:
		try:
			decoder = IppDecoder(data)
			version, operation, request_id = decoder.read_header()
			attrs = decoder.read_attributes()

			if operation == IppOperation.PRINT_JOB:
				return await self.handle_print_job(
					version, request_id, attrs, decoder.get_document_data()
				)
			if operation == IppOperation.GET_PRINTER_ATTRIBUTES:
				return self.handle_get_printer_attributes(version, request_id)
			if operation == IppOperation.GET_JOBS:
				return self.handle_get_jobs(version, request_id)
			if operation == IppOperation.GET_JOB_ATTRIBUTES:
				return self.handle_get_job_attributes(version, request_id, attrs)
			if operation == IppOperation.CANCEL_JOB:
				return self.handle_cancel_job(version, request_id, attrs)
			if operation == IppOperation.VALIDATE_JOB:
				return self.handle_validate_job(version, request_id)

			return build_error_response(version, request_id, IppStatus.SERVER_ERROR_INTERNAL)
		except Exception as exc:
			_logger.error("Error processing IPP request: %s", exc)
			return build_error_response((1, 1), 0, IppStatus.SERVER_ERROR_INTERNAL)

	async def handle_print_job(
		self,
		version: tuple[int, int],
		request_id: int,
		attrs: dict[str, list[tuple[int, bytes]]],
		document: bytes,
	) -> bytes:
		if self.delay > 0:
			await asyncio.sleep(self.delay)
		if random.random() < self.fail_rate:
			return build_error_response(version, request_id, IppStatus.SERVER_ERROR_INTERNAL)

		decoder = IppDecoder(b"")
		job_name = decoder.get_string(attrs, "job-name", "unknown")
		doc_format = decoder.get_string(attrs, "document-format", "application/octet-stream")

		job_id = self._next_job_id
		self._next_job_id += 1
		job = IppJob(
			id=job_id,
			name=job_name,
			state=IppJobState.COMPLETED,
			document_format=doc_format,
			data=document,
		)
		self.jobs[job_id] = job

		if self.save_dir:
			self.save_dir.mkdir(parents=True, exist_ok=True)
			ext = self.format_to_extension(doc_format)
			safe_name = job_name.replace(" ", "_").replace("/", "_")
			filename = f"job_{job_id}_{safe_name}.{ext}"
			(self.save_dir / filename).write_bytes(document)

		enc = IppEncoder()
		enc.write_header(version, IppStatus.OK, request_id)
		enc.write_tag(IppTag.OPERATION)
		enc.write_charset("attributes-charset", "utf-8")
		enc.write_language("attributes-natural-language", "en")
		enc.write_tag(IppTag.JOB)
		enc.write_integer(IppTag.INTEGER, "job-id", job_id)
		enc.write_uri("job-uri", f"ipp://{self.host}:{self.actual_port}/jobs/{job_id}")
		enc.write_enum("job-state", IppJobState.COMPLETED)
		enc.write_tag(IppTag.END)
		return enc.get_bytes()

	def handle_get_printer_attributes(
		self,
		version: tuple[int, int],
		request_id: int,
	) -> bytes:
		enc = IppEncoder()
		enc.write_header(version, IppStatus.OK, request_id)
		enc.write_tag(IppTag.OPERATION)
		enc.write_charset("attributes-charset", "utf-8")
		enc.write_language("attributes-natural-language", "en")
		enc.write_tag(IppTag.PRINTER)
		enc.write_name("printer-name", self.name)

		state_map = {"idle": 3, "processing": 4, "error": 5}
		enc.write_enum("printer-state", state_map.get(self.printer_state, 5))

		if self.state_reasons:
			for index, reason in enumerate(self.state_reasons):
				if index == 0:
					enc.write_keyword("printer-state-reasons", reason)
				else:
					enc.write_attribute(IppTag.KEYWORD, "", reason.encode("utf-8"))
		else:
			enc.write_keyword("printer-state-reasons", "none")

		enc.write_uri("printer-uri-supported", self.uri)
		if self.page_count > 0:
			enc.write_integer(IppTag.INTEGER, "impressions-count", self.page_count)
		for fmt in self.supported_formats:
			enc.write_mime_type("document-format-supported", fmt)

		enc.write_tag(IppTag.END)
		return enc.get_bytes()

	def handle_get_jobs(self, version: tuple[int, int], request_id: int) -> bytes:
		enc = IppEncoder()
		enc.write_header(version, IppStatus.OK, request_id)
		enc.write_tag(IppTag.OPERATION)
		enc.write_charset("attributes-charset", "utf-8")
		enc.write_language("attributes-natural-language", "en")
		for job in self.jobs.values():
			enc.write_tag(IppTag.JOB)
			enc.write_integer(IppTag.INTEGER, "job-id", job.id)
			enc.write_name("job-name", job.name)
			enc.write_enum("job-state", job.state)
		enc.write_tag(IppTag.END)
		return enc.get_bytes()

	def handle_get_job_attributes(
		self,
		version: tuple[int, int],
		request_id: int,
		attrs: dict[str, list[tuple[int, bytes]]],
	) -> bytes:
		decoder = IppDecoder(b"")
		job_id = decoder.get_integer(attrs, "job-id", 0)
		job = self.jobs.get(job_id)
		if not job:
			return build_error_response(version, request_id, IppStatus.CLIENT_ERROR_NOT_FOUND)

		enc = IppEncoder()
		enc.write_header(version, IppStatus.OK, request_id)
		enc.write_tag(IppTag.OPERATION)
		enc.write_charset("attributes-charset", "utf-8")
		enc.write_language("attributes-natural-language", "en")
		enc.write_tag(IppTag.JOB)
		enc.write_integer(IppTag.INTEGER, "job-id", job.id)
		enc.write_name("job-name", job.name)
		enc.write_enum("job-state", job.state)
		enc.write_tag(IppTag.END)
		return enc.get_bytes()

	def handle_cancel_job(
		self,
		version: tuple[int, int],
		request_id: int,
		attrs: dict[str, list[tuple[int, bytes]]],
	) -> bytes:
		decoder = IppDecoder(b"")
		job_id = decoder.get_integer(attrs, "job-id", 0)
		if job_id in self.jobs:
			self.jobs[job_id].state = IppJobState.CANCELED

		enc = IppEncoder()
		enc.write_header(version, IppStatus.OK, request_id)
		enc.write_tag(IppTag.OPERATION)
		enc.write_charset("attributes-charset", "utf-8")
		enc.write_language("attributes-natural-language", "en")
		enc.write_tag(IppTag.END)
		return enc.get_bytes()

	def handle_validate_job(self, version: tuple[int, int], request_id: int) -> bytes:
		enc = IppEncoder()
		enc.write_header(version, IppStatus.OK, request_id)
		enc.write_tag(IppTag.OPERATION)
		enc.write_charset("attributes-charset", "utf-8")
		enc.write_language("attributes-natural-language", "en")
		enc.write_tag(IppTag.END)
		return enc.get_bytes()

	def format_to_extension(self, mime_type: str) -> str:
		mapping = {
			"application/pdf": "pdf",
			"image/pwg-raster": "pwg",
			"image/urf": "urf",
			"application/postscript": "ps",
			"text/plain": "txt",
		}
		return mapping.get(mime_type, "bin")

	def clear(self) -> None:
		self.jobs.clear()

	@property
	def job_count(self) -> int:
		return len(self.jobs)

	@property
	def last_job(self) -> IppJob | None:
		if not self.jobs:
			return None
		return self.jobs[max(self.jobs.keys())]

	@property
	def total_bytes(self) -> int:
		return sum(len(job.data) for job in self.jobs.values())

	@property
	def jobs_received(self) -> list[IppJob]:
		return [self.jobs[job_id] for job_id in sorted(self.jobs.keys())]


@asynccontextmanager
async def ipp_printer(**kwargs):
	mock = IppPrinterMock(**kwargs)
	await mock.start()
	try:
		yield mock
	finally:
		await mock.stop()


MockIppPrinter = IppPrinterMock
