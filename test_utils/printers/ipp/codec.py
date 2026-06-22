"""IPP protocol codec - encoding and decoding IPP messages."""

import struct
from enum import IntEnum


class IppOperation(IntEnum):
	"""IPP operation codes."""

	PRINT_JOB = 0x0002
	PRINT_URI = 0x0003
	VALIDATE_JOB = 0x0004
	CREATE_JOB = 0x0005
	SEND_DOCUMENT = 0x0006
	SEND_URI = 0x0007
	CANCEL_JOB = 0x0008
	GET_JOB_ATTRIBUTES = 0x0009
	GET_JOBS = 0x000A
	GET_PRINTER_ATTRIBUTES = 0x000B
	HOLD_JOB = 0x000C
	RELEASE_JOB = 0x000D
	RESTART_JOB = 0x000E
	PAUSE_PRINTER = 0x0010
	RESUME_PRINTER = 0x0011
	PURGE_JOBS = 0x0012


class IppStatus(IntEnum):
	"""IPP status codes."""

	OK = 0x0000
	OK_IGNORED = 0x0001
	OK_CONFLICTING = 0x0002
	OK_EVENTS_COMPLETE = 0x0007

	# Client errors (0x04xx)
	CLIENT_ERROR_BAD_REQUEST = 0x0400
	CLIENT_ERROR_FORBIDDEN = 0x0401
	CLIENT_ERROR_NOT_AUTHENTICATED = 0x0402
	CLIENT_ERROR_NOT_AUTHORIZED = 0x0403
	CLIENT_ERROR_NOT_POSSIBLE = 0x0404
	CLIENT_ERROR_TIMEOUT = 0x0405
	CLIENT_ERROR_NOT_FOUND = 0x0406
	CLIENT_ERROR_GONE = 0x0407
	CLIENT_ERROR_REQUEST_ENTITY = 0x0408
	CLIENT_ERROR_REQUEST_VALUE = 0x0409
	CLIENT_ERROR_DOCUMENT_FORMAT_NOT_SUPPORTED = 0x040A
	CLIENT_ERROR_ATTRIBUTES_OR_VALUES = 0x040B
	CLIENT_ERROR_URI_SCHEME = 0x040C
	CLIENT_ERROR_CHARSET = 0x040D
	CLIENT_ERROR_CONFLICTING = 0x040E
	CLIENT_ERROR_COMPRESSION_NOT_SUPPORTED = 0x040F
	CLIENT_ERROR_COMPRESSION_ERROR = 0x0410
	CLIENT_ERROR_DOCUMENT_FORMAT_ERROR = 0x0411
	CLIENT_ERROR_DOCUMENT_ACCESS_ERROR = 0x0412

	# Server errors (0x05xx)
	SERVER_ERROR_INTERNAL = 0x0500
	SERVER_ERROR_OPERATION_NOT_SUPPORTED = 0x0501
	SERVER_ERROR_SERVICE_UNAVAILABLE = 0x0502
	SERVER_ERROR_VERSION_NOT_SUPPORTED = 0x0503
	SERVER_ERROR_DEVICE_ERROR = 0x0504
	SERVER_ERROR_TEMPORARY = 0x0505
	SERVER_ERROR_NOT_ACCEPTING_JOBS = 0x0506
	SERVER_ERROR_BUSY = 0x0507
	SERVER_ERROR_JOB_CANCELED = 0x0508
	SERVER_ERROR_MULTIPLE_DOCS_NOT_SUPPORTED = 0x0509


class IppTag(IntEnum):
	"""IPP attribute tags."""

	# Delimiter tags
	OPERATION = 0x01
	JOB = 0x02
	END = 0x03
	PRINTER = 0x04
	UNSUPPORTED_GROUP = 0x05

	# Value tags
	UNSUPPORTED = 0x10
	UNKNOWN = 0x12
	NO_VALUE = 0x13

	# Integer tags
	INTEGER = 0x21
	BOOLEAN = 0x22
	ENUM = 0x23

	# Octet string tags
	OCTET_STRING = 0x30
	DATE_TIME = 0x31
	RESOLUTION = 0x32
	RANGE_OF_INTEGER = 0x33
	COLLECTION = 0x34
	TEXT_WITH_LANGUAGE = 0x35
	NAME_WITH_LANGUAGE = 0x36

	# Character string tags
	TEXT = 0x41
	NAME = 0x42
	KEYWORD = 0x44
	URI = 0x45
	URI_SCHEME = 0x46
	CHARSET = 0x47
	LANGUAGE = 0x48
	MIME_TYPE = 0x49


class IppJobState(IntEnum):
	"""IPP job states."""

	PENDING = 3
	PENDING_HELD = 4
	PROCESSING = 5
	PROCESSING_STOPPED = 6
	CANCELED = 7
	ABORTED = 8
	COMPLETED = 9


class IppPrinterState(IntEnum):
	"""IPP printer states."""

	IDLE = 3
	PROCESSING = 4
	STOPPED = 5


class IppEncoder:
	"""Encode IPP response messages."""

	def __init__(self) -> None:
		self.buffer = bytearray()

	def write_header(
		self,
		version: tuple[int, int],
		status_or_op: int,
		request_id: int,
	) -> None:
		"""Write IPP header.

		Args:
		        version: IPP version tuple (major, minor).
		        status_or_op: Status code (for responses) or operation (for requests).
		        request_id: Request ID to echo back.
		"""
		self.buffer.extend(
			struct.pack(
				">BBhI",
				version[0],
				version[1],
				status_or_op,
				request_id,
			)
		)

	def write_tag(self, tag: int) -> None:
		"""Write a delimiter tag."""
		self.buffer.append(tag)

	def write_attribute(self, tag: int, name: str, value: bytes) -> None:
		"""Write a raw attribute.

		Args:
		        tag: Value tag.
		        name: Attribute name (empty string for additional values).
		        value: Raw value bytes.
		"""
		self.buffer.append(tag)
		self.buffer.extend(struct.pack(">H", len(name)))
		self.buffer.extend(name.encode("utf-8"))
		self.buffer.extend(struct.pack(">H", len(value)))
		self.buffer.extend(value)

	def write_string(self, tag: int, name: str, value: str) -> None:
		"""Write a string attribute."""
		self.write_attribute(tag, name, value.encode("utf-8"))

	def write_integer(self, tag: int, name: str, value: int) -> None:
		"""Write an integer attribute (4 bytes, signed)."""
		self.write_attribute(tag, name, struct.pack(">i", value))

	def write_boolean(self, name: str, value: bool) -> None:
		"""Write a boolean attribute."""
		self.write_attribute(IppTag.BOOLEAN, name, bytes([1 if value else 0]))

	def write_enum(self, name: str, value: int) -> None:
		"""Write an enum attribute."""
		self.write_attribute(IppTag.ENUM, name, struct.pack(">i", value))

	def write_keyword(self, name: str, value: str) -> None:
		"""Write a keyword attribute."""
		self.write_string(IppTag.KEYWORD, name, value)

	def write_uri(self, name: str, value: str) -> None:
		"""Write a URI attribute."""
		self.write_string(IppTag.URI, name, value)

	def write_name(self, name: str, value: str) -> None:
		"""Write a name attribute."""
		self.write_string(IppTag.NAME, name, value)

	def write_text(self, name: str, value: str) -> None:
		"""Write a text attribute."""
		self.write_string(IppTag.TEXT, name, value)

	def write_mime_type(self, name: str, value: str) -> None:
		"""Write a MIME type attribute."""
		self.write_string(IppTag.MIME_TYPE, name, value)

	def write_charset(self, name: str, value: str) -> None:
		"""Write a charset attribute."""
		self.write_string(IppTag.CHARSET, name, value)

	def write_language(self, name: str, value: str) -> None:
		"""Write a natural language attribute."""
		self.write_string(IppTag.LANGUAGE, name, value)

	def write_range(self, name: str, lower: int, upper: int) -> None:
		"""Write a range-of-integer attribute."""
		self.write_attribute(
			IppTag.RANGE_OF_INTEGER,
			name,
			struct.pack(">ii", lower, upper),
		)

	def write_resolution(
		self,
		name: str,
		xres: int,
		yres: int,
		units: int = 3,
	) -> None:
		"""Write a resolution attribute.

		Args:
		        name: Attribute name.
		        xres: X resolution.
		        yres: Y resolution.
		        units: 3 = dpi, 4 = dots per cm.
		"""
		self.write_attribute(
			IppTag.RESOLUTION,
			name,
			struct.pack(">iib", xres, yres, units),
		)

	def get_bytes(self) -> bytes:
		"""Get the encoded message."""
		return bytes(self.buffer)


class IppDecoder:
	"""Decode IPP request messages."""

	def __init__(self, data: bytes) -> None:
		self.data = data
		self.pos = 0

	def read_header(self) -> tuple[tuple[int, int], int, int]:
		"""Read IPP header.

		Returns:
		        Tuple of (version, operation/status, request_id).
		"""
		if len(self.data) < 8:
			raise ValueError("IPP message too short for header")

		major, minor, op, req_id = struct.unpack_from(">BBhI", self.data, self.pos)
		self.pos += 8
		return (major, minor), op, req_id

	def read_attributes(self) -> dict[str, list[tuple[int, bytes]]]:
		"""Read all attributes until END tag or data section.

		Returns:
		        Dict mapping attribute names to list of (tag, value) tuples.
		        Multiple values for same attribute are collected in the list.
		"""
		attrs: dict[str, list[tuple[int, bytes]]] = {}
		current_name = ""

		while self.pos < len(self.data):
			tag = self.data[self.pos]
			self.pos += 1

			# End of attributes
			if tag == IppTag.END:
				break

			# Delimiter tags - skip but continue
			if tag in (
				IppTag.OPERATION,
				IppTag.JOB,
				IppTag.PRINTER,
				IppTag.UNSUPPORTED_GROUP,
			):
				continue

			# Skip unknown delimiter tags (< 0x10)
			if tag < 0x10:
				continue

			# Value tag - read name and value
			if self.pos + 2 > len(self.data):
				break

			name_len = struct.unpack_from(">H", self.data, self.pos)[0]
			self.pos += 2

			if self.pos + name_len > len(self.data):
				break

			name = self.data[self.pos : self.pos + name_len].decode("utf-8")
			self.pos += name_len

			if self.pos + 2 > len(self.data):
				break

			value_len = struct.unpack_from(">H", self.data, self.pos)[0]
			self.pos += 2

			if self.pos + value_len > len(self.data):
				break

			value = self.data[self.pos : self.pos + value_len]
			self.pos += value_len

			# Empty name means additional value for previous attribute
			if name:
				current_name = name
			if current_name:
				if current_name not in attrs:
					attrs[current_name] = []
				attrs[current_name].append((tag, value))

		return attrs

	def get_document_data(self) -> bytes:
		"""Get remaining data after attributes (the document)."""
		return self.data[self.pos :]

	# Convenience methods for extracting typed values

	def get_string(
		self,
		attrs: dict[str, list[tuple[int, bytes]]],
		name: str,
		default: str = "",
	) -> str:
		"""Get a string attribute value."""
		if name not in attrs or not attrs[name]:
			return default
		return attrs[name][0][1].decode("utf-8")

	def get_integer(
		self,
		attrs: dict[str, list[tuple[int, bytes]]],
		name: str,
		default: int = 0,
	) -> int:
		"""Get an integer attribute value."""
		if name not in attrs or not attrs[name]:
			return default
		value = attrs[name][0][1]
		if len(value) == 4:
			return int(struct.unpack(">i", value)[0])
		return default

	def get_boolean(
		self,
		attrs: dict[str, list[tuple[int, bytes]]],
		name: str,
		default: bool = False,
	) -> bool:
		"""Get a boolean attribute value."""
		if name not in attrs or not attrs[name]:
			return default
		value = attrs[name][0][1]
		return value[0] != 0 if value else default


def build_error_response(
	version: tuple[int, int],
	request_id: int,
	status: IppStatus,
	message: str | None = None,
) -> bytes:
	"""Build a standard IPP error response.

	Args:
	        version: IPP version tuple.
	        request_id: Request ID to echo.
	        status: Error status code.
	        message: Optional status message.

	Returns:
	        Encoded IPP response bytes.
	"""
	enc = IppEncoder()
	enc.write_header(version, status, request_id)
	enc.write_tag(IppTag.OPERATION)
	enc.write_charset("attributes-charset", "utf-8")
	enc.write_language("attributes-natural-language", "en")
	if message:
		enc.write_text("status-message", message)
	enc.write_tag(IppTag.END)
	return enc.get_bytes()


def build_success_response(
	version: tuple[int, int],
	request_id: int,
) -> IppEncoder:
	"""Start building a success response with standard attributes.

	Returns an encoder with header and operation attributes started.
	Caller should add additional attributes and call write_tag(END).

	Args:
	        version: IPP version tuple.
	        request_id: Request ID to echo.

	Returns:
	        IppEncoder ready for additional attributes.
	"""
	enc = IppEncoder()
	enc.write_header(version, IppStatus.OK, request_id)
	enc.write_tag(IppTag.OPERATION)
	enc.write_charset("attributes-charset", "utf-8")
	enc.write_language("attributes-natural-language", "en")
	return enc
