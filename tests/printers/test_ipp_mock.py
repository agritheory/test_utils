import struct
from pathlib import Path

import httpx
import pytest

from test_utils.printers.ipp.codec import (
	IppDecoder,
	IppEncoder,
	IppOperation,
	IppStatus,
	IppTag,
)
from test_utils.printers.mocks.ipp import IppPrinterMock


def build_print_job_request(document: bytes, printer_uri: str) -> bytes:
	enc = IppEncoder()
	enc.write_header((1, 1), IppOperation.PRINT_JOB, 1)
	enc.write_tag(IppTag.OPERATION)
	enc.write_charset("attributes-charset", "utf-8")
	enc.write_language("attributes-natural-language", "en")
	enc.write_uri("printer-uri", printer_uri)
	enc.write_name("job-name", "Test Document")
	enc.write_mime_type("document-format", "application/pdf")
	enc.write_tag(IppTag.END)
	return enc.get_bytes() + document


def build_get_printer_attributes_request(printer_uri: str) -> bytes:
	enc = IppEncoder()
	enc.write_header((1, 1), IppOperation.GET_PRINTER_ATTRIBUTES, 2)
	enc.write_tag(IppTag.OPERATION)
	enc.write_charset("attributes-charset", "utf-8")
	enc.write_language("attributes-natural-language", "en")
	enc.write_uri("printer-uri", printer_uri)
	enc.write_tag(IppTag.END)
	return enc.get_bytes()


@pytest.mark.asyncio
async def test_ipp_mock_print_job_and_get_attributes(tmp_path: Path):
	mock = IppPrinterMock(port=0, save_dir=tmp_path, name="PDF")
	await mock.start()
	try:
		document = b"%PDF-1.4 fake"
		print_body = build_print_job_request(document, mock.uri)
		async with httpx.AsyncClient() as client:
			response = await client.post(
				f"http://{mock.address}{mock.printer_path}",
				content=print_body,
				headers={"Content-Type": "application/ipp"},
				timeout=5.0,
			)
			assert response.status_code == 200

			decoder = IppDecoder(response.content)
			version, status, req_id = decoder.read_header()
			assert status == IppStatus.OK
			attrs = decoder.read_attributes()
			assert decoder.get_integer(attrs, "job-id") == 1

			attr_response = await client.post(
				f"http://{mock.address}{mock.printer_path}",
				content=build_get_printer_attributes_request(mock.uri),
				headers={"Content-Type": "application/ipp"},
				timeout=5.0,
			)
			attr_decoder = IppDecoder(attr_response.content)
			_, attr_status, _ = attr_decoder.read_header()
			assert attr_status == IppStatus.OK

		assert mock.job_count == 1
		assert mock.last_job is not None
		assert mock.last_job.data == document
		assert list(tmp_path.glob("*.pdf"))
	finally:
		await mock.stop()


@pytest.mark.asyncio
async def test_ipp_mock_cancel_job():
	mock = IppPrinterMock(port=0)
	await mock.start()
	try:
		document = b"test"
		async with httpx.AsyncClient() as client:
			await client.post(
				f"http://{mock.address}{mock.printer_path}",
				content=build_print_job_request(document, mock.uri),
				headers={"Content-Type": "application/ipp"},
				timeout=5.0,
			)

			enc = IppEncoder()
			enc.write_header((1, 1), IppOperation.CANCEL_JOB, 3)
			enc.write_tag(IppTag.OPERATION)
			enc.write_charset("attributes-charset", "utf-8")
			enc.write_language("attributes-natural-language", "en")
			enc.write_integer(IppTag.INTEGER, "job-id", 1)
			enc.write_tag(IppTag.END)

			response = await client.post(
				f"http://{mock.address}{mock.printer_path}",
				content=enc.get_bytes(),
				headers={"Content-Type": "application/ipp"},
				timeout=5.0,
			)
			decoder = IppDecoder(response.content)
			_, status, _ = decoder.read_header()
			assert status == IppStatus.OK
			assert mock.jobs[1].state == 7
	finally:
		await mock.stop()
