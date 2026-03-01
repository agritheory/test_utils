#!/bin/bash
set -e

OUTPUT_DIR="$HOME/changelogs"
mkdir -p "$OUTPUT_DIR"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "$GITHUB_TOKEN" ]; then
	echo "Error: GITHUB_TOKEN environment variable is not set."
	exit 1
fi

# Format: "github_repo:local_app_path"
APPS=(
	"agritheory/saml:$HOME/cranberry/apps/saml"
	"agritheory/approvals:$HOME/cranberry/apps/approvals"
	"agritheory/shipstation_integration:$HOME/quercus/apps/shipstation_integration"
	"agritheory/frappe_vault:$HOME/uhdei/apps/frappe_vault"
	"agritheory/cloud_storage:$HOME/uhdei/apps/cloud_storage"
	"agritheory/autoreader:$HOME/acacia/apps/autoreader"
	"agritheory/electronic_payments:$HOME/poplar/apps/electronic_payments"
	"agritheory/inventory_tools:$HOME/salix/apps/inventory_tools"
	"agritheory/beam:$HOME/pinyon/apps/beam"
)

TOTAL=${#APPS[@]}
DONE=0
SKIPPED=0

echo "========================================"
echo " Retrospective Changelog Generator"
echo " Output: $OUTPUT_DIR"
echo " Model:  mistral:7b-instruct-q4_K_M"
echo " Apps:   $TOTAL"
echo "========================================"
echo ""

for ENTRY in "${APPS[@]}"; do
	REPO="${ENTRY%%:*}"
	LOCAL_PATH="${ENTRY##*:}"
	APP_NAME="${REPO#agritheory/}"
	OUTPUT_FILE="$OUTPUT_DIR/$APP_NAME.md"

	if [ -f "$OUTPUT_FILE" ]; then
		echo "[ SKIP ] $REPO — already exists: $OUTPUT_FILE"
		SKIPPED=$((SKIPPED + 1))
		continue
	fi

	echo ""
	echo "[ $((DONE + SKIPPED + 1))/$TOTAL ] Processing $REPO"
	echo "  Checking out version-15 in $LOCAL_PATH..."
	git -C "$LOCAL_PATH" checkout version-15

	python "$SCRIPT_DIR/retrospective.py" "$REPO" \
		--github-token "$GITHUB_TOKEN" \
		--use-ollama \
		--ollama-model "mistral:7b-instruct-q4_K_M" \
		--output "$OUTPUT_FILE"

	DONE=$((DONE + 1))
	echo "  Done → $OUTPUT_FILE"
done

echo ""
echo "========================================"
echo " Complete: $DONE generated, $SKIPPED skipped"
echo " Files in $OUTPUT_DIR:"
ls -lh "$OUTPUT_DIR"
echo "========================================"
