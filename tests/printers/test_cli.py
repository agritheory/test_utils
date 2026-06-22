import socket

import pytest

from test_utils.printers.cli import build_parser, run_raw


@pytest.mark.asyncio
async def test_run_raw_prints_jobs(tmp_path):
	mock = await run_raw("127.0.0.1", 0, tmp_path)
	try:
		with socket.create_connection((mock.host, mock.actual_port), timeout=2) as conn:
			conn.sendall(b"^XA^XZ")
		assert await mock.wait_for_jobs(1, timeout=2.0)
		assert list(tmp_path.glob("*.raw"))
	finally:
		await mock.stop()


def test_build_parser_raw_defaults():
	parser = build_parser()
	args = parser.parse_args(["raw", "--save-dir", "/tmp/prints"])
	assert args.command == "raw"
	assert args.port == 9100
	assert args.save_dir == "/tmp/prints"
