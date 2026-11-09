# fedq-server

A PostgreSQL wire-protocol server over the federated engine: any Postgres
client (psql, tokio-postgres, psycopg, JDBC, Npgsql) queries the engine as a
standalone database. Simple and extended query protocols are supported;
results stream as Postgres rows.

## Run

```
cargo build --release -p fedq-server
target/release/fedq-server --config config/example_config.yaml --listen 127.0.0.1:6432
psql "host=127.0.0.1 port=6432 user=anyone dbname=main"
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

`server.auth` is explicit: `trust` (the default) or `scram`. Trust is an
unauthenticated development mode with implicit server-superuser authority.
SCRAM-SHA-256 requires bootstrap users including a global superuser;
configuring bootstrap credentials under `trust` raises at startup. Unknown
users and wrong passwords are refused with SQLSTATE 28P01.

```
server:
  auth: scram
  users:
    - name: analyst
      salt: <base64>
      salted_password: <base64>
```

## Databases

Every connection selects a database (`dbname`), and `main` is bootstrapped
automatically. A missing, unknown, or unauthorized database is rejected
before ReadyForQuery (SQLSTATE 3D000 for a missing catalog, 42501 for a
permission failure); the server never substitutes another database. Each
database owns its metadata and results under
`<storage-root>/databases/<database-id>/`; the server catalog
(`server.sqlite`) holds users, the database registry, and CONNECT/ADMIN
grants. `CREATE/DROP DATABASE` and global user administration are
server-superuser operations performed from a `main` connection. A database
ADMIN manages datasets, views, materialized views, aliases, and object
grants only inside its own database; connector provisioning remains
server-superuser work.

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
