# fedq-server

A PostgreSQL wire-protocol server over the federated engine: any Postgres
client (psql, tokio-postgres, psycopg, JDBC, Npgsql) queries the engine as a
standalone database. Simple and extended query protocols are supported;
results stream as Postgres rows.

## Run

```
cargo build --release -p fedq-server
target/release/fedq-server --config config/example_config.yaml --listen 127.0.0.1:6432
psql "host=127.0.0.1 port=6432 user=anyone dbname=fedq"
```

## Options

- `--config <path>`  (required) the engine YAML: datasources, optimizer
  settings, and the optional `server:` auth section.
- `--listen <host:port>`  listen address; default `127.0.0.1:5432`.
- `--help` / `-h`  usage.
- `hash-password <user> <password>`  subcommand: prints the SCRAM-SHA-256
  credential block (base64 salt + salted password, never plaintext) to paste
  under `server: users:` in the config.

## Auth

No `server:` section (or an empty user list) means trust auth: any user name
connects without a password. With users configured, authentication is
SCRAM-SHA-256; unknown users and wrong passwords are refused with SQLSTATE
28P01.

```
server:
  users:
    - name: analyst
      salt: <base64>
      salted_password: <base64>
```

## Behavior and limits

- One engine Runtime per connection, pinned to its own worker thread; the
  server is read-only over its sources.
- Extended-protocol parameters are spliced as SQL literals at Bind (the
  engine has no native placeholders); binary-format parameters of unmapped
  types are refused loudly by SQLSTATE naming the type.
- Date/timestamp/decimal results render as canonical text; a binary-format
  request for those columns is refused rather than mislabeled.
- Cancellation returns SQLSTATE 57014 and the connection survives; the
  abandoned query finishes on its worker and the result is discarded (the
  engine exposes no mid-query interruption seam).
