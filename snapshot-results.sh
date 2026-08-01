#!/bin/bash
# Snapshot the isolated-run results every 60s so no single mistake can lose them.
while true; do
    if [ -s /workspace/isolated-results.jsonl ]; then
        cp --reflink=auto -f /workspace/isolated-results.jsonl \
            /workspace/isolated-results.backup.jsonl.tmp 2>/dev/null \
            && mv -f /workspace/isolated-results.backup.jsonl.tmp \
                     /workspace/isolated-results.backup.jsonl
    fi
    sleep 60
done
