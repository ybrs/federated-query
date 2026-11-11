# Automatic substitution: additional correctness checks

The [main substitution chapter](07-automatic-substitution.md) demonstrates a
cached three-source computation joined to live PostgreSQL revenue, with outer
ORDER BY and LIMIT. These additional checks exercise the matching boundaries.

The real server checks compared every answer with DuckDB over the source
fixtures. Cached executions read one local accelerator source; refusal cases
read all three upstream sources.

| Client query/control | Substitutes? | `use_count` before -> after |
| --- | --- | --- |
| Original three-source support SELECT | Yes | 0 -> 1 |
| Lowercase keywords and different whitespace | Yes | 1 -> 2 |
| Category changed from gameplay to billing | No | 2 -> 2 |
| Output alias changed to ticket_count | No | 2 -> 2 |
| Support SELECT inside an outer ordered query | Yes | 2 -> 3 |
| Original SELECT with substitution disabled | No | 3 -> 3 |
| Same SELECT after RESET | Yes | 3 -> 4 |

EXPLAIN left the counters unchanged. An unknown column raised while the view
was registered and did not advance the counter. The changed predicate returned
billing-ticket results, and the changed output alias was preserved.

To repeat these checks against the server from [setup](setup.md):

```bash
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/verify_substitution.py
```

The checker refuses to replace an existing `studio_substitution_check` view,
removes its own view on exit, and does not mutate upstream rows. The full
[evidence](evidence/substitution.json) retains the seven queries, answers,
plans, executed fragments, counter changes, and invalid-query check.

[Contents](README.md) | [Automatic substitution showcase](07-automatic-substitution.md)
