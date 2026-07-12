#!/usr/bin/env bash
# Fetch the OFFICIAL prebuilt libduckdb matching the libduckdb-sys crate pinned
# in Cargo.lock, into .duckdb-lib/<version>/ with a stable `current` symlink.
#
# WHY THIS EXISTS: the duckdb crate's "bundled" feature compiles the DuckDB C++
# amalgamation under the active cargo profile - a dev build produces an -O0
# DuckDB (~10x slower execution) and a release build costs ~10 minutes of C++
# compile per clean build. We NEVER compile DuckDB inside cargo. The workspace
# links this prebuilt library via DUCKDB_LIB_DIR/DUCKDB_INCLUDE_DIR + rpath
# (see .cargo/config.toml); tools/fq-lint rejects any "bundled" feature.
#
# Idempotent: re-running with the library already in place is a no-op.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lock_file="$repo_root/Cargo.lock"

# Derive the DuckDB release version from the pinned libduckdb-sys crate version.
# The crate encodes it as 1.MMmPP.x, e.g. 1.10504.0 -> DuckDB v1.5.4.
sys_version="$(awk '/^name = "libduckdb-sys"$/{getline; gsub(/version = |"/,""); print; exit}' "$lock_file")"
if [ -z "$sys_version" ]; then
    echo "ERROR: libduckdb-sys not found in $lock_file" >&2
    exit 1
fi
encoded="$(echo "$sys_version" | cut -d. -f2)"
duck_version="v$((encoded / 10000)).$(((encoded % 10000) / 100)).$((encoded % 100))"

lib_dir="$repo_root/.duckdb-lib/$duck_version"
current_link="$repo_root/.duckdb-lib/current"

if [ -f "$lib_dir/libduckdb.so" ]; then
    echo "libduckdb $duck_version already present at $lib_dir"
else
    echo "Fetching official libduckdb $duck_version (libduckdb-sys $sys_version) ..."
    mkdir -p "$lib_dir"
    zip_url="https://github.com/duckdb/duckdb/releases/download/$duck_version/libduckdb-linux-amd64.zip"
    tmp_zip="$(mktemp /tmp/libduckdb-XXXXXX.zip)"
    curl -fsSL --retry 3 -o "$tmp_zip" "$zip_url"
    unzip -o -q "$tmp_zip" -d "$lib_dir"
    rm -f "$tmp_zip"
fi

# Verify the binary self-reports the version the crate expects. A mismatched
# library would fail at runtime in confusing ways; die loudly here instead.
embedded="$(python3 - "$lib_dir/libduckdb.so" <<'PY'
import ctypes, sys
lib = ctypes.CDLL(sys.argv[1])
lib.duckdb_library_version.restype = ctypes.c_char_p
print(lib.duckdb_library_version().decode())
PY
)"
if [ "$embedded" != "$duck_version" ]; then
    echo "ERROR: $lib_dir/libduckdb.so reports $embedded, crate expects $duck_version" >&2
    exit 1
fi

ln -sfn "$lib_dir" "$current_link"
echo "OK: $current_link -> $lib_dir (verified $embedded)"
