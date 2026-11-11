# Setup: Docker sources and FedQ server

Run commands from the repository root on Linux. Use an activated Python virtualenv,
Docker with Compose v2 or newer, the PostgreSQL client `psql`, and permission
to use your Docker daemon.
The scripts use only dependencies already listed in `requirements.txt`.

```bash
source /path/to/your/venv/bin/activate
make install
make build
docker compose -f tutorials/game-studio/compose.yaml up -d --wait
```

The Compose project is `fedq-game-studio`. Its three databases use dedicated
containers and loopback ports: PostgreSQL 55432, MySQL 53306, ClickHouse HTTP
58123. The included credentials are `tutorial` / `tutorial` for this local
example. No existing database is used. Container data survives a stop/start;
the [reset command](reset-and-extend.md) removes it.

On a host where Docker requires sudo, use `sudo docker compose` for the
Compose commands and set this before running the loader:

```bash
export GAME_STUDIO_DOCKER='sudo -n docker'
```

## Generate and load the data

```bash
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/generate.py
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/seed.py
```

Generation writes typed Parquet fixtures to the ignored `generated/` folder.
The loader creates tables and inserts their rows into the three actual
databases. Only `generated/acquisition/web_events.parquet` is registered as
the analytics source. The other fixture files let you audit the input and
check answers independently; FedQ reads the database tables for those domains.

The loader writes `generated/fedq.yaml` with all four connections, absolute
local paths, and the PostgreSQL ADBC library from the active virtualenv.
Missing dependencies and failed inserts raise. Loading twice raises on the
existing tables: use the [explicit reset](reset-and-extend.md) before loading another dataset.

## Connect through FedQ server

Keep the server running in one terminal:

```bash
target/release/fedq-server \
  --config tutorials/game-studio/generated/fedq.yaml \
  --listen 127.0.0.1:56432
```

In a second terminal, activate the same virtualenv and connect to FedQ with
`psql`. This is the FedQ port; PostgreSQL billing remains on port 55432 behind
the engine.

```bash
source /path/to/your/venv/bin/activate
export FEDQ_DSN='host=127.0.0.1 port=56432 dbname=main user=analyst'
psql "$FEDQ_DSN" -X -v ON_ERROR_STOP=1
```

The generated config uses the default trust authentication for this local,
loopback-only example. `analyst` is the connection name, not a provisioned
restricted login. Configure SCRAM and grants when deploying a shared server.

The source prefixes are catalog registrations. These commands expose the
ownership information that binding uses before it can split a query:

```sql
SHOW DATASOURCES;
SHOW TABLES FROM billing;
DESCRIBE billing.public.players;
DESCRIBE crm.support.tickets;
DESCRIBE gameplay.gameplay.events;
DESCRIBE acquisition.main.web_events;
```

`billing.public.players` is described as:

```text
      column       |   type    | nullable
-------------------+-----------+----------
 player_id         | BIGINT    | NO
 registered_at     | TIMESTAMP | NO
 subscription_plan | VARCHAR   | NO
```

The first chapter's player keys and plan names are resolved from that schema;
the binder does not infer ownership from whichever source happens to answer.

---

[Contents](README.md) | [Previous: 10-data-contracts.md](10-data-contracts.md) | [Next: inspection-and-verification.md](inspection-and-verification.md)
