# Printer mocks

`test_utils.printers` provides a stdlib-only IPP codec and mock raw/IPP print servers for integration tests and manual development.

## Install

```bash
pip install "git+https://github.com/agritheory/test_utils.git@v1.26.0"
```

## CLI

```bash
mock-printer raw --save-dir /tmp/prints
mock-printer ipp --save-dir /tmp/prints --name PDF --port 8631
mock-printer both --save-dir /tmp/prints
```

The CLI prints ready-to-copy URIs:

```
Raw:  socket://127.0.0.1:44895
IPP:  ipp://127.0.0.1:8631/ipp/print
```

## Programmatic use

Async (pytest-asyncio, Kenaf):

```python
from test_utils.printers import raw_printer, ipp_printer

async with raw_printer(save_dir="/tmp/prints") as printer:
    print(printer.device_uri)

async with ipp_printer(name="PDF", save_dir="/tmp/prints") as printer:
    print(printer.uri)
```

Sync (Frappe / BEAM pytest):

```python
from test_utils.printers import raw_printer_sync, ipp_printer_sync

with raw_printer_sync(save_dir="/tmp/prints") as printer:
    payload = printer.wait_for_payload()
```

## CUPS registration

Point CUPS queues at the mock URIs:

```bash
lpadmin -p BEAM_TEST_RAW -E -v socket://127.0.0.1:9100 -m raw
lpadmin -p BEAM_TEST_IPP -E -v ipp://127.0.0.1:8631/ipp/print -m everywhere
cupsenable BEAM_TEST_RAW && cupsaccept BEAM_TEST_RAW
```

## IPP codec

The codec in `test_utils.printers.ipp.codec` is general-purpose (not test-only). Kenaf re-exports it from `src/ipp_codec.py`.

## Kenaf codec duplication

Default: Kenaf imports the shared codec from `test_utils` via a thin re-export.

If the Kenaf appliance build cannot depend on `test_utils` at runtime, keep a vendored copy of `ipp/codec.py` in Kenaf and sync it in CI. Mocks can remain a dev dependency.
