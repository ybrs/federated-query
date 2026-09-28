# Start the repository's test server and export its selected connection port.
ensure_test_postgres() {
  local root="$1"
  local port_file="${root}/.fedq-test-postgres-port"

  "${root}/scripts/run-postgres.sh" || return $?
  if [ ! -s "${port_file}" ]; then
    echo "The test PostgreSQL server did not save its port." >&2
    return 1
  fi

  PGPORT="$(cat "${port_file}")"
  POSTGRES_PORT="${PGPORT}"
  FQ_TEST_PG_URL="host=localhost port=${PGPORT} user=postgres dbname=postgres"
  export PGPORT POSTGRES_PORT FQ_TEST_PG_URL

  if [ -d "${root}/.test-harness" ]; then
    "${VIRTUAL_ENV}/bin/python3" -c \
      'from tests.e2e_federated.build_harness import write_config; write_config()'
  fi
}
