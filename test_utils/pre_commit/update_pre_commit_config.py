import subprocess
import sys


def main():
	try:
		# Run the pre-commit autoupdate with --repo (path to test_utils) to update pre-commit config
		result = subprocess.run(
			["pre-commit", "autoupdate", "--repo", "https://github.com/agritheory/test_utils"],
			capture_output=True,
			text=True,
		)

		print(result.stdout)
		if result.stderr:
			print(result.stderr)

	except Exception as e:
		print(f"Error: {e}")
		sys.exit(1)


if __name__ == "__main__":
	main()
