import argparse
import sys
from typing import Sequence

try:
	from frappe.translate import get_untranslated, update_translations
except Exception as e:
	raise(e)


def add_translations(lang, app):
	try:
		untranslated_file = "untranslated_strings"
		translated_file = "translated_strings"
		get_untranslated(lang=lang, untranslated_file=untranslated_file, app=app)
		update_translations(lang=lang, untranslated_file=untranslated_file, translated_file=translated_file, app=app)
	except Exception as e:
		print(f"An error occurred while translating for lang '{lang}' and app '{app}': {e}")
		sys.exit(0)


def main(argv: Sequence[str] = None):
	parser = argparse.ArgumentParser()
	parser.add_argument('filenames', nargs='*')
	parser.add_argument('--lang', action='append', help='Language to translate strings')
	parser.add_argument('--app', action='append', help='App to get untranslated string and translate them')
	args = parser.parse_args(argv)

	lang = args.lang[0]
	app = args.app[0]
	add_translations(lang, app)