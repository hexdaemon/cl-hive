#!/usr/bin/env bash
# sign-repo.sh — Regenerate and sign the manifest after changes
# Run this after any repo changes before pushing

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCHON_DIR="${ARCHON_CONFIG_DIR:-$HOME/.config/hex/archon}"

cd "$REPO_DIR"

export ARCHON_GATEKEEPER_URL="${ARCHON_GATEKEEPER_URL:-https://archon.technology}"
if [[ -z "${ARCHON_PASSPHRASE:-}" ]]; then
  echo "Error: ARCHON_PASSPHRASE not set"
  exit 1
fi

MY_DID="did:cid:bagaaieratn3qejd6mr4y2bk3nliriafoyeftt74tkl7il6bbvakfdupahkla"

echo "=== Generating manifest ==="
cat > "$ARCHON_DIR/manifest.json" << MANIFEST
{
  "@context": "https://w3id.org/security/v2",
  "type": "RepoManifest",
  "issuer": "$MY_DID",
  "created": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "repository": "https://github.com/lightning-goats/cl-hive",
  "files": [
$(find . -type f ! -path "./.git/*" ! -name "manifest*.json" | sort | while read -r f; do
  hash=$(sha256sum "$f" | cut -d' ' -f1)
  echo "    {\"path\": \"$f\", \"sha256\": \"$hash\"},"
done | sed '$ s/,$//')
  ]
}
MANIFEST

echo "=== Signing manifest ==="
cd "$ARCHON_DIR"
npx @didcid/keymaster sign-file manifest.json > "$REPO_DIR/manifest.json" 2>&1

echo "=== Verifying ==="
npx @didcid/keymaster verify-file "$REPO_DIR/manifest.json" 2>&1

echo "=== Done ==="
echo "Manifest signed. Commit manifest.json with your changes."
