#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: geofabrik_harvest.sh <download_url> <output_path>" >&2
  exit 2
fi

url="$1"
out="$2"
mkdir -p "$(dirname "$out")"

curl -fsSL "$url" -o "$out"
echo "Downloaded $url -> $out"
