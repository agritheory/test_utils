import struct

import pytest

from test_utils.printers.ipp.codec import (
	IppDecoder,
	IppEncoder,
	IppOperation,
	IppStatus,
	IppTag,
	build_error_response,
	build_success_response,
)


class TestIppEncoder:
	def test_write_header(self):
		enc = IppEncoder()
		enc.write_header((1, 1), IppStatus.OK, 12345)

		data = enc.get_bytes()
		assert len(data) == 8
		assert data[0] == 1
		assert data[1] == 1
		assert struct.unpack(">h", data[2:4])[0] == IppStatus.OK
		assert struct.unpack(">I", data[4:8])[0] == 12345

	def test_write_tag(self):
		enc = IppEncoder()
		enc.write_tag(IppTag.OPERATION)
		enc.write_tag(IppTag.END)
		assert enc.get_bytes() == bytes([IppTag.OPERATION, IppTag.END])

	def test_write_string(self):
		enc = IppEncoder()
		enc.write_string(IppTag.TEXT, "test-attr", "hello")
		data = enc.get_bytes()
		assert data[0] == IppTag.TEXT
		name_len = struct.unpack(">H", data[1:3])[0]
		assert name_len == 9
		assert data[3 : 3 + name_len].decode() == "test-attr"

	def test_write_integer(self):
		enc = IppEncoder()
		enc.write_integer(IppTag.INTEGER, "count", 42)
		data = enc.get_bytes()
		value_start = 1 + 2 + 5 + 2
		assert struct.unpack(">i", data[value_start : value_start + 4])[0] == 42


class TestIppDecoder:
	def test_read_header(self):
		data = struct.pack(">BBhI", 2, 0, IppOperation.PRINT_JOB, 54321)
		decoder = IppDecoder(data)
		version, op, req_id = decoder.read_header()
		assert version == (2, 0)
		assert op == IppOperation.PRINT_JOB
		assert req_id == 54321

	def test_read_header_too_short(self):
		decoder = IppDecoder(b"\x01\x01")
		with pytest.raises(ValueError, match="too short"):
			decoder.read_header()

	def test_get_document_data(self):
		enc = IppEncoder()
		enc.write_tag(IppTag.OPERATION)
		enc.write_string(IppTag.CHARSET, "attributes-charset", "utf-8")
		enc.write_tag(IppTag.END)
		document = b"This is the document content"
		decoder = IppDecoder(enc.get_bytes() + document)
		decoder.read_attributes()
		assert decoder.get_document_data() == document


class TestHelperFunctions:
	def test_build_error_response(self):
		response = build_error_response(
			(1, 1),
			123,
			IppStatus.CLIENT_ERROR_NOT_FOUND,
			"Printer not found",
		)
		decoder = IppDecoder(response)
		version, status, req_id = decoder.read_header()
		assert version == (1, 1)
		assert status == IppStatus.CLIENT_ERROR_NOT_FOUND
		assert req_id == 123

	def test_build_success_response(self):
		enc = build_success_response((1, 1), 456)
		enc.write_tag(IppTag.END)
		decoder = IppDecoder(enc.get_bytes())
		version, status, req_id = decoder.read_header()
		assert status == IppStatus.OK
		assert req_id == 456
