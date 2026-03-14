#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data"

mkdir -p "$DATA_DIR"

download_and_unpack() {
  local name="$1"
  local url="$2"
  local gz_path="${DATA_DIR}/${name}.txt.gz"
  local txt_path="${DATA_DIR}/${name}.txt"

  echo "Downloading ${name}..."
  if command -v curl >/dev/null 2>&1; then
    curl -L -o "$gz_path" "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$gz_path" "$url"
  else
    echo "Error: neither curl nor wget is available."
    exit 1
  fi

  echo "Unpacking ${name}..."
  gunzip "$gz_path"

  if [[ ! -f "$txt_path" ]]; then
    echo "Error: expected ${txt_path} after unpacking."
    exit 1
  fi
}

download_and_unpack "livejournal" "https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz"
download_and_unpack "twitter" "https://snap.stanford.edu/data/twitter-2010.txt.gz"
download_and_unpack "friendster" "https://snap.stanford.edu/data/bigdata/communities/com-friendster.ungraph.txt.gz"

echo "Generating rmat27..."
python3 "${SCRIPT_DIR}/gen_rmat27.py"

if [[ ! -f "${DATA_DIR}/rmat27.txt" ]]; then
  echo "Error: rmat27.txt not generated. Check networkit install."
  exit 1
fi

echo "All datasets are ready in ${DATA_DIR}."
