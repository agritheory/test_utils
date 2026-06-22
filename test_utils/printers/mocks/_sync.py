import asyncio
import threading
from contextlib import contextmanager
from typing import Any
from collections.abc import Iterator

from test_utils.printers.mocks.ipp import IppPrinterMock, ipp_printer
from test_utils.printers.mocks.raw import RawPrinterMock, raw_printer


class AsyncServiceThread:
	"""Background asyncio loop for sync test helpers."""

	def __init__(self) -> None:
		self.loop = asyncio.new_event_loop()
		self.thread = threading.Thread(target=self.run_loop, daemon=True)
		self.ready = threading.Event()

	def run_loop(self) -> None:
		asyncio.set_event_loop(self.loop)
		self.ready.set()
		self.loop.run_forever()

	def start(self) -> None:
		self.thread.start()
		self.ready.wait()

	def run(self, coro):
		return asyncio.run_coroutine_threadsafe(coro, self.loop).result()

	def stop(self) -> None:
		self.loop.call_soon_threadsafe(self.loop.stop)
		self.thread.join(timeout=5)
		self.loop.close()


SERVICE_THREAD: AsyncServiceThread | None = None


def service_thread() -> AsyncServiceThread:
	global SERVICE_THREAD
	if SERVICE_THREAD is None:
		SERVICE_THREAD = AsyncServiceThread()
		SERVICE_THREAD.start()
	return SERVICE_THREAD


async def open_async_context(cm) -> tuple[Any, Any]:
	mock = await cm.__aenter__()
	return mock, cm


async def close_async_context(cm) -> None:
	await cm.__aexit__(None, None, None)


@contextmanager
def raw_printer_sync(**kwargs: Any) -> Iterator[RawPrinterMock]:
	thread = service_thread()
	cm = raw_printer(**kwargs)
	mock, cm = thread.run(open_async_context(cm))
	try:
		yield mock
	finally:
		thread.run(close_async_context(cm))


@contextmanager
def ipp_printer_sync(**kwargs: Any) -> Iterator[IppPrinterMock]:
	thread = service_thread()
	cm = ipp_printer(**kwargs)
	mock, cm = thread.run(open_async_context(cm))
	try:
		yield mock
	finally:
		thread.run(close_async_context(cm))
