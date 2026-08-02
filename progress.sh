#!/bin/bash
# What every process of a parallel suite run is doing RIGHT NOW.
#
# Run it any time while run_e2e_parallel.sh is going. Corpus shards report the
# percentage pytest last printed; plan variants report from their progress file,
# because each is a single test that prints nothing until it ends.
set -uo pipefail
LOGS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-e2e-logs"
[ -d "${LOGS}" ] || { echo "no run-e2e-logs/ - nothing has run"; exit 0; }
printf '%-12s %-8s %s\n' PROCESS STATE DETAIL
for log in "${LOGS}"/*.log; do
    name="$(basename "${log}" .log)"
    if [ -f "${LOGS}/${name}.exit" ]; then
        printf '%-12s %-8s %s\n' "${name}" \
            "done($(cat "${LOGS}/${name}.exit"))" \
            "$(grep -aE '[0-9]+ (passed|failed)' "${log}" | tail -1)"
        continue
    fi
    detail="$(tail -1 "${LOGS}/${name}.progress" 2>/dev/null)"
    [ -z "${detail}" ] && detail="$(grep -oE '\[ *[0-9]+%\]' "${log}" | tail -1)"
    [ -z "${detail}" ] && detail="starting"
    printf '%-12s %-8s %s\n' "${name}" running "${detail}"
done
