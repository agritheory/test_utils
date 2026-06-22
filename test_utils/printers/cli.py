import argparse
import asyncio
import signal
from pathlib import Path

from test_utils.printers.mocks.ipp import IppPrinterMock
from test_utils.printers.mocks.raw import RawPrinterMock


async def run_raw(host: str, port: int, save_dir: Path | None) -> RawPrinterMock:
	mock = RawPrinterMock(host=host, port=port, save_dir=save_dir)
	await mock.start()
	print(f"Raw:  {mock.device_uri}")
	if save_dir:
		print(f"Saving raw jobs to {save_dir}")
	return mock


async def run_ipp(
	host: str,
	port: int,
	save_dir: Path | None,
	name: str,
	printer_path: str,
) -> IppPrinterMock:
	mock = IppPrinterMock(
		host=host,
		port=port,
		save_dir=save_dir,
		name=name,
		printer_path=printer_path,
	)
	await mock.start()
	print(f"IPP:  {mock.uri}")
	if save_dir:
		print(f"Saving IPP jobs to {save_dir}")
	return mock


async def serve(args: argparse.Namespace) -> None:
	save_dir = Path(args.save_dir).expanduser() if args.save_dir else None
	mocks: list[RawPrinterMock | IppPrinterMock] = []

	if args.command in ("raw", "both"):
		mocks.append(
			await run_raw(args.host, args.port if args.command == "raw" else 0, save_dir)
		)
	if args.command in ("ipp", "both"):
		ipp_port = args.port if args.command == "ipp" else 0
		printer_path = args.printer_path
		if args.command == "both" and args.name == "Mock IPP Printer":
			args.name = "Mock IPP"
		mocks.append(
			await run_ipp(
				args.host,
				ipp_port,
				save_dir,
				args.name,
				printer_path,
			)
		)

	stop_event = asyncio.Event()

	def request_stop(*_signum) -> None:
		stop_event.set()

	for sig in (signal.SIGINT, signal.SIGTERM):
		signal.signal(sig, request_stop)

	print("Press Ctrl+C to stop.")
	await stop_event.wait()

	for mock in mocks:
		await mock.stop()


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description="Run mock raw and/or IPP printers for manual testing."
	)
	subparsers = parser.add_subparsers(dest="command", required=True)

	def add_common(subparser: argparse.ArgumentParser) -> None:
		subparser.add_argument("--host", default="127.0.0.1")
		subparser.add_argument(
			"--port", type=int, default=0, help="Listen port (0 = OS-assigned)."
		)
		subparser.add_argument(
			"--save-dir",
			help="Directory where received print jobs are written.",
		)

	raw_parser = subparsers.add_parser("raw", help="Run a raw socket printer mock.")
	add_common(raw_parser)
	raw_parser.set_defaults(port=9100)

	ipp_parser = subparsers.add_parser("ipp", help="Run an IPP printer mock.")
	add_common(ipp_parser)
	ipp_parser.add_argument("--name", default="Mock IPP Printer")
	ipp_parser.add_argument("--printer-path", default="/ipp/print")
	ipp_parser.set_defaults(port=8631)

	both_parser = subparsers.add_parser(
		"both", help="Run raw and IPP printer mocks together."
	)
	add_common(both_parser)
	both_parser.add_argument("--name", default="Mock IPP")
	both_parser.add_argument("--printer-path", default="/ipp/print")

	return parser


def main() -> None:
	parser = build_parser()
	args = parser.parse_args()
	asyncio.run(serve(args))


if __name__ == "__main__":
	main()
