import argparse
import sys
from collections.abc import Sequence


def is_frappe_environment():
	"""
	Check if the current environment is a Frappe environment.

	Returns:
	        bool: True if Frappe is available, False otherwise
	"""
	try:
		import frappe

		return True
	except ImportError:
		return False


def add_translations(lang, app):
	try:
		from frappe.translate import get_untranslated, update_translations

		untranslated_file = "untranslated_strings"
		translated_file = "translated_strings"
		get_untranslated(lang=lang, untranslated_file=untranslated_file, app=app)
		update_translations(
			lang=lang,
			untranslated_file=untranslated_file,
			translated_file=translated_file,
			app=app,
		)
		print(f"Successfully processed translations for language '{lang}' and app '{app}'")
	except Exception as e:
		print(f"An error occurred while translating for lang '{lang}' and app '{app}': {e}")
		sys.exit(1)


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument("filenames", nargs="*")
	parser.add_argument("--lang", action="append", help="Language to translate strings")
	parser.add_argument(
		"--app", action="append", help="App to get untranslated string and translate them"
	)
	args = parser.parse_args(argv)

	lang = args.lang[0]
	app = args.app[0]

	if not is_frappe_environment():
		print("Error: Frappe environment not detected.")
		print("This script must be run from within a Frappe bench environment.")
		sys.exit(1)

	add_translations(lang, app)
