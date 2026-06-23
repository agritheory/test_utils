import asyncio
from pathlib import Path

import pytest

from test_utils.printers.mocks.raw import RawPrinterMock


@pytest.mark.asyncio
async def test_raw_mock_receives_payload_and_saves_file(tmp_path: Path):
	mock = RawPrinterMock(port=0, save_dir=tmp_path)
	await mock.start()
	try:
		reader, writer = await asyncio.open_connection(mock.host, mock.actual_port)
		writer.write(b"^XA^FDTest^FS^XZ")
		await writer.drain()
		writer.close()
		await writer.wait_closed()

		assert await mock.wait_for_jobs(1, timeout=2.0)
		assert mock.last_text().startswith("^XA")
		assert list(tmp_path.glob("*.raw"))
	finally:
		await mock.stop()


def test_raw_printer_sync_wrapper(tmp_path: Path):
	import socket

	from test_utils.printers.mocks._sync import raw_printer_sync

	with raw_printer_sync(port=0, save_dir=tmp_path) as mock:
		with socket.create_connection((mock.host, mock.actual_port), timeout=2) as conn:
			conn.sendall(b"hello")
		payload = mock.wait_for_payload(timeout=2.0)
		assert payload == b"hello"
