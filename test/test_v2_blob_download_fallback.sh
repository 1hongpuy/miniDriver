#!/usr/bin/env bash

set -euo pipefail

app_js="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/www-v2/app.js"

if grep -Fq 'if (!window.showSaveFilePicker) throw new Error' "${app_js}"; then
    echo "FAIL: download path still rejects browsers without File System Access API" >&2
    exit 1
fi

if ! grep -Fq 'URL.createObjectURL' "${app_js}"; then
    echo "FAIL: Blob fallback does not create a browser download URL" >&2
    exit 1
fi

if ! grep -Fq 'new Blob(chunkBlobs' "${app_js}"; then
    echo "FAIL: Blob fallback does not assemble verified chunks" >&2
    exit 1
fi

echo "PASS: V2 frontend has a Blob download fallback"
