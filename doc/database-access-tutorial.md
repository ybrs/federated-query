# Tutorial: databases, users, grants, and querying as a user

You have a DuckDB file with application tables. This walks through putting a
project's data in its own database, creating a login for a colleague, giving
that login access to exactly what they need, and then running queries as that
colleague over the PostgreSQL wire. You will see the denials as well as the
successes.

Every command and output below is real: each was run against the release
binaries and pasted word for word. Substitute your own paths.

Three tools are used:

- `fedq`, the command-line client. It runs built into the engine, so it acts
  as an administrator. Use it for setup.
- SQL statements such as `CREATE DATABASE`, `CREATE USER`, and `GRANT` for
  all the admin work.
- `fedq-server` together with `psql`, the normal PostgreSQL client, for
  connecting as a particular user. This is where passwords and grants are
  actually checked.

## 0. Seed data and a bootstrap config

Create a DuckDB file with two tables:

```python
import duckdb
con = duckdb.connect('/tmp/dbtut/shop.duckdb')
con.execute("CREATE TABLE orders (id INTEGER, total INTEGER)")
con.execute("INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)")
con.execute("CREATE TABLE lineitem (id INTEGER, qty INTEGER)")
con.execute("INSERT INTO lineitem VALUES (1, 4), (2, 7)")
con.close()
```

The config file holds a password hash, not a password. You produce the hash by
running this command with the password you want. It prints a YAML block that
you paste into the config. A fresh random salt is used on each run, so your
output differs from mine - that is expected.

```
fedq-server hash-password --superuser admin 'adminpw'
```

```
    - name: admin
      verifier: SCRAM-SHA-256$4096:biJjML1GY0EOB/QYwFoHLA==$xpF+8lBAZYvTk1bgridA6YyB3qJv6GjpbMpXp6tTIAo=:DR7MX8xf8aaKOqACUREgWMpPwG3LuF4NhaPZbEgLxhI=
      superuser: true
```

Copy that block into `engine.yaml`. The `server` section says how logins are
checked: `scram` means a password is required. If you list users but leave
`auth` at `trust`, the engine refuses to start, because that combination
would let everyone in with no check.

```yaml
storage:
  root: /tmp/dbtut/state

server:
  auth: scram
  users:
    - name: admin
      verifier: SCRAM-SHA-256$4096:biJjML1GY0EOB/QYwFoHLA==$xpF+8lBAZYvTk1bgridA6YyB3qJv6GjpbMpXp6tTIAo=:DR7MX8xf8aaKOqACUREgWMpPwG3LuF4NhaPZbEgLxhI=
      superuser: true
  scram_iterations: 4096

datasources:
  duck:
    type: duckdb
    path: /tmp/dbtut/shop.duckdb
```

Where does the engine keep everything? Under `storage.root` it creates two
kinds of files. One file, `server.sqlite`, holds the list of users, the list
of databases, and who is allowed to connect to which database. Then, for every
database you create, the engine creates a folder named after an internal ID
(a random string, not the name you gave the database) holding that database's
own files: its table definitions, its stored query results, and its own grant
records. The name you choose and the folder on disk are linked internally, so
two databases with similar names still get separate folders.

## 1. Create a database

The engine always starts with one database called `main`:

```
fedq --config /tmp/dbtut/engine.yaml --command "SHOW DATABASES"
```

```
+------+
| name |
+------+
| main |
+------+
```

Create a second database for the project:

```
fedq --config /tmp/dbtut/engine.yaml --command "CREATE DATABASE analytics"
```

```
+-----------------+
| status          |
+-----------------+
| CREATE DATABASE |
+-----------------+
```

```
fedq --config /tmp/dbtut/engine.yaml --command "SHOW DATABASES"
```

```
+-----------+
| name      |
+-----------+
| analytics |
| main      |
+-----------+
```

## 2. Create a user

A user is created once for the whole engine, not inside one database. After
creating the user, you decide separately which databases that user may enter.
Create a login for your colleague:

```
fedq --config /tmp/dbtut/engine.yaml --command "CREATE USER analyst WITH PASSWORD 'analystpw'"
```

```
+-------------+
| status      |
+-------------+
| CREATE USER |
+-------------+
```

```
fedq --config /tmp/dbtut/engine.yaml --command "SHOW USERS"
```

```
+---------+-----------+-------------------------------------+
| name    | superuser | created                             |
+---------+-----------+-------------------------------------+
| admin   | true      | 2026-11-10T09:00:21.166854258+00:00 |
| analyst | false     | 2026-11-10T09:00:21.382489021+00:00 |
+---------+-----------+-------------------------------------+
```

The password hash is not displayed back. Creating the same user twice fails,
and dropping a user also removes every grant given to that user, so a deleted
login leaves no permission behind for someone reusing the name later.

## 3. Let the user into the database, then to the table

Access has two layers, granted with two statements. First, allow the person to
connect to the database at all:

```
fedq --config /tmp/dbtut/engine.yaml --command "GRANT CONNECT ON DATABASE analytics TO analyst"
```

```
+--------+
| status |
+--------+
| GRANT  |
+--------+
```

Connecting is not reading: this alone lets `analyst` open a session on
`analytics`, but every table in it is still forbidden. Section 6 shows what
that refusal looks like.

Second, point the new database at your data and allow one table. A database you
create starts empty - it does not see the sources listed in the YAML file
(only `main` uses those). Register the DuckDB file inside `analytics` with
`--database`; the registration is stored, so it survives restarts:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "CREATE DATASOURCE duck TYPE duckdb WITH (path = '/tmp/dbtut/shop.duckdb')"
```

```
+-------------------+
| status            |
+-------------------+
| CREATE DATASOURCE |
+-------------------+
```

Now allow exactly one table. This grant lives inside `analytics` and has no
effect anywhere else:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "GRANT SELECT ON TABLE duck.main.orders TO analyst"
```

```
+--------+
| status |
+--------+
| GRANT  |
+--------+
```

Check what was granted:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "SHOW GRANTS FOR analyst"
```

```
+---------+-----------+-------------+------------------+------------+-------------------------------------+
| grantee | privilege | object_kind | object_path      | granted_by | created                             |
+---------+-----------+-------------+------------------+------------+-------------------------------------+
| analyst | SELECT    | table       | duck.main.orders | embedded   | 2026-11-10T09:00:21.710891888+00:00 |
+---------+-----------+-------------+------------------+------------+-------------------------------------+
```

`analyst` may now read one table in one database. Nothing else.

Two rules about issuing grants. First, the order does not matter: granting
the table before the connect permission ends in exactly the same place, since
both are simply stored until they are checked. Second, one statement carries
one privilege on one object for one user (tracked as
https://github.com/ybrs/fedq-private/issues/9). Asking for two things at once is
rejected:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "GRANT SELECT, CONNECT ON TABLE duck.main.orders TO analyst"
```

```
parse error: parse error: expected 'ON', found ','
```

## 4. Start the server

The remaining checks connect as `analyst`, so start the wire server now:

```
fedq-server --config /tmp/dbtut/engine.yaml --listen 127.0.0.1:5544
```

## 5. Wider access: whole sources, views, and administering the database

One table at a time gets tedious. To open every current and future table under
a source, grant on the source instead of the table:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "GRANT SELECT ON DATASOURCE duck TO analyst"
```

```
+--------+
| status |
+--------+
| GRANT  |
+--------+
```

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "SHOW GRANTS FOR analyst"
```

```
+---------+-----------+-------------+------------------+------------+-------------------------------------+
| grantee | privilege | object_kind | object_path      | granted_by | created                             |
+---------+-----------+-------------+------------------+------------+-------------------------------------+
| analyst | SELECT    | datasource  | duck             | embedded   | 2026-11-10T09:22:01.194064606+00:00 |
| analyst | SELECT    | table       | duck.main.orders | embedded   | 2026-11-10T09:00:21.710891888+00:00 |
+---------+-----------+-------------+------------------+------------+-------------------------------------+
```

With this, `analyst` reads both `orders` and `lineitem`. To go back to the
single table, revoke the wide grant - the narrow one is still there:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "REVOKE SELECT ON DATASOURCE duck FROM analyst"
```

```
+--------+
| status |
+--------+
| REVOKE |
+--------+
```

Views are granted one by one, each under its own name. Create a view as the
administrator:

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "CREATE VIEW big_orders AS SELECT id, total FROM duck.main.orders WHERE total > 15"
```

```
+-------------+
| status      |
+-------------+
| CREATE VIEW |
+-------------+
```

The source-level table grant does not extend to it, so the colleague is
refused:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "SELECT id FROM big_orders"
```

```
ERROR:  bind error: Table not found: big_orders
```

Grant the view by name (the underlying table is already covered, so only the
view grant is missing):

```
fedq --config /tmp/dbtut/engine.yaml --database analytics \
  --command "GRANT SELECT ON VIEW big_orders TO analyst"
```

```
+--------+
| status |
+--------+
| GRANT  |
+--------+
```

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "SELECT id, total FROM big_orders ORDER BY id"
```

```
 id | total
----+-------
  2 |    20
  3 |    30
(2 rows)
```

Finally, there is no grant that lets someone create only views (for now, see
https://github.com/ybrs/fedq-private/issues/10). Building
anything new - a view, a datasource, or a grant for someone else - requires
making the user an administrator of that database. That power stays inside
the database: it grants nothing on `main` and nothing server-wide.

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "CREATE VIEW mine AS SELECT id FROM duck.main.orders"
```

```
ERROR:  permission denied: database ADMIN required
```

```
fedq --config /tmp/dbtut/engine.yaml \
  --command "GRANT ADMIN ON DATABASE analytics TO analyst"
```

```
+--------+
| status |
+--------+
| GRANT  |
+--------+
```

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "CREATE VIEW mine AS SELECT id FROM duck.main.orders"
```

```
   status
-------------
 CREATE VIEW
(1 row)
```

With database admin rights the colleague can also grant other users inside
`analytics`:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "GRANT SELECT ON TABLE duck.main.lineitem TO admin"
```

```
 status
--------
 GRANT
(1 row)
```

Hand that power back when the work is done - the views built with it stay:

```
fedq --config /tmp/dbtut/engine.yaml \
  --command "REVOKE ADMIN ON DATABASE analytics FROM analyst"
```

```
+--------+
| status |
+--------+
| REVOKE |
+--------+
```

## 6. Run queries as the user

Connect as `analyst` to `analytics`. The granted table reads normally:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "SELECT id, total FROM duck.main.orders ORDER BY id"
```

```
 id | total
----+-------
  1 |    10
  2 |    20
  3 |    30
(3 rows)
```

The other table is refused. The error says the table was not found, which is
deliberate: a user with no rights gets the same message for a table they may
not read as for a table that does not exist, so they cannot probe for table
names by guessing:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "SELECT id, qty FROM duck.main.lineitem"
```

```
ERROR:  bind error: Table not found: duck.main.lineitem
```

Listing databases shows only the databases this user may enter:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "SHOW DATABASES"
```

```
   name
-----------
 analytics
(1 row)
```

And connecting to a database the user was not let into is refused before any
SQL is even read:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=main" -c "SELECT 1"
```

```
psql: error: connection to server at "127.0.0.1", port 5544 failed: ERROR:  permission denied: database CONNECT required
```

## 7. Take access away again

Taking a grant back works immediately. There is nothing to restart, and the
user does not have to disconnect first: their next attempt simply fails. As
`admin` on `main`:

```
PGPASSWORD=adminpw psql "host=127.0.0.1 port=5544 user=admin dbname=main" \
  -c "REVOKE CONNECT ON DATABASE analytics FROM analyst"
```

```
 status
--------
 REVOKE
```

The colleague's next connection is refused:

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -c "SELECT id FROM duck.main.orders"
```

```
psql: error: connection to server at "127.0.0.1", port 5544 failed: ERROR:  permission denied: database CONNECT required
```

Grant it back and the queries work again:

```
PGPASSWORD=adminpw psql "host=127.0.0.1 port=5544 user=admin dbname=main" \
  -c "GRANT CONNECT ON DATABASE analytics TO analyst"
```

```
PGPASSWORD=analystpw psql "host=127.0.0.1 port=5544 user=analyst dbname=analytics" \
  -t -c "SELECT count(*) FROM duck.main.orders"
```

```
        3
```

## 8. Cleaning up

Removing a database is an administrator's job and must be issued while
connected to `main`. It deletes only that database's folder; every other
database is left alone:

```
fedq --config /tmp/dbtut/engine.yaml --command "DROP DATABASE analytics"
```

```
+---------------+
| status        |
+---------------+
| DROP DATABASE |
+---------------+
```

Two things that look like they should work do not, on purpose. Issuing the
drop while connected to any other database fails, because only `main` may
remove databases. And `main` itself cannot be dropped - the engine refuses,
since `main` holds the shared sources and the bootstrap state. If anyone is
still connected to the database you are dropping, the drop is refused until
their sessions close.

## Notes

- The order of grants does not matter: `CONNECT` first or `SELECT` first ends
  in the same place. Both must be present - with only `CONNECT` the user
  connects but every table query fails, and with only `SELECT` the user
  cannot open a session at all.
- Each `GRANT` carries one privilege on one object for one user. Broader
  statements are rejected by the parser instead of being partly applied.
- A refusal does not reveal whether the object exists. A forbidden table and
  a missing table produce the same message.
- The same flow works from Python: `fedq.Runtime("engine.yaml",
  database="analytics")` opens a session on that database, and `execute(sql)`
  runs one statement.
