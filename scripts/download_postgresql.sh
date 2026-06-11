#!/bin/bash
# Downloads a prebuilt, self-contained PostgreSQL binary distribution used by the
# local test harness. No root or system package install is required: the binaries
# are unpacked into ./postgres-17 and driven by run-postgres.sh / stop-postgres.sh.
#
# Source: https://github.com/theseus-rs/postgresql-binaries (static builds).
# Platform (OS + arch) is auto-detected, so the same script works on Linux CI
# machines and macOS developer laptops.

set -euo pipefail

PG_VERSION="${PG_VERSION:-17.4.0}"
TARGET_DIR="postgres-17"

# Map `uname` output to the release's target triple.
os_name="$(uname -s)"
arch_name="$(uname -m)"

case "${os_name}-${arch_name}" in
  Linux-x86_64)   triple="x86_64-unknown-linux-gnu" ;;
  Linux-aarch64)  triple="aarch64-unknown-linux-gnu" ;;
  Darwin-arm64)   triple="aarch64-apple-darwin" ;;
  Darwin-x86_64)  triple="x86_64-apple-darwin" ;;
  *)
    echo "Unsupported platform: ${os_name}-${arch_name}" >&2
    exit 1
    ;;
esac

archive="postgresql-${PG_VERSION}-${triple}.tar.gz"
url="https://github.com/theseus-rs/postgresql-binaries/releases/download/${PG_VERSION}/${archive}"

if [ -d "${TARGET_DIR}" ]; then
  echo "${TARGET_DIR} already exists; skipping download."
  exit 0
fi

echo "Downloading ${url}"
curl -fSL -o "${archive}" "${url}"

echo "Extracting ${archive}"
tar zxf "${archive}"
mv "postgresql-${PG_VERSION}-${triple}" "${TARGET_DIR}"
rm -f "${archive}"

echo "PostgreSQL ${PG_VERSION} ready in ./${TARGET_DIR}"
