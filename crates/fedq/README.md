# fedq

The command-line front end over the federated query engine. It runs a single SQL
statement and prints the result, or opens an interactive shell. Execution is
delegated to `fq_runtime::Runtime`; this crate owns argument parsing, result
rendering, and the readline loop.

## Options

```
fedq --config <YAML> [--command <SQL> | --file <PATH>] [--format table|csv|json]
```

- `--config, -c <YAML>` (required): the YAML engine config (datasources,
  optimizer, cost, ...), the same format the server and tests use.
- `--command <SQL>`: run one statement, then exit (one-shot mode). Mutually
  exclusive with `--file`.
- `--file <PATH>`: run the file's contents as one statement, then exit.
- `--format table|csv|json` (default `table`): the output format.

With neither `--command` nor `--file`, fedq opens the interactive shell.

Exit status: `0` on success, `1` on a config/runtime/execution error (the message
prints to stderr), `2` on an argument error (from clap).

## One-shot examples

```
# Aligned table (the default)
fedq --config engine.yaml --command "SELECT id, name FROM shop.main.items ORDER BY id"
+----+--------+
| id | name   |
+----+--------+
| 1  | apple  |
| 2  | banana |
| 3  | cherry |
+----+--------+

# CSV (a null cell is an empty field; commas/quotes/newlines are quoted)
fedq --config engine.yaml --format csv --command "SELECT id, name FROM shop.main.items"
id,name
1,apple
2,banana
3,cherry

# JSON (integers/floats/booleans are typed; a null cell is JSON null)
fedq --config engine.yaml --format json --command "SELECT id, name FROM shop.main.items WHERE id = 1"
[
  {
    "id": 1,
    "name": "apple"
  }
]

# Run a statement from a file
fedq --config engine.yaml --file report.sql
```

`EXPLAIN` needs no special handling: `EXPLAIN <query>` renders as its single
`plan` column.

## Interactive shell

Running with no `--command`/`--file` opens a readline REPL:

```
fedq --config engine.yaml
fedq interactive shell. End statements with ';'. Type \q to quit.
fedq> SELECT id, name
...>  FROM shop.main.items
...>  ORDER BY id;
+----+--------+
| id | name   |
+----+--------+
| 1  | apple  |
| 2  | banana |
| 3  | cherry |
+----+--------+
(3 rows)
fedq>
```

- A statement spans as many lines as needed and runs when a line leaves the
  buffer ending in `;`.
- `\q` quits; Ctrl-D on an empty prompt also quits.
- `\timing` toggles a per-statement elapsed-time line.
- Ctrl-C abandons the statement in progress and returns to a fresh prompt.
- A statement that fails prints its error to stderr and the shell continues.
