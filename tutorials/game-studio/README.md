# Game studio: a FedQ showcase

Follow SQL across PostgreSQL billing, MySQL support, ClickHouse gameplay, and
Parquet acquisition data. Each chapter includes the business SQL, actual source
queries and plans, and an explanation of what FedQ executes locally.

Start with [setup](setup.md) to run the examples through FedQ server, or read
the chapters without starting the databases. Commands run from the repository root.

## The chapters

| Chapter | What it shows |
| --- | --- |
| [0. The studio and execution pipeline](00-studio-and-execution.md) | Data ownership, binding, decorrelation, pushdowns, Arrow streams, and local execution |
| [1. Support by subscription plan](01-support-by-plan.md) | Follow a PostgreSQL/MySQL query from SQL to the final aggregation |
| [2. Correlated questions](02-correlated-questions.md) | How EXISTS becomes a semi-join and independent source queries |
| [3. Acquisition to revenue](03-acquisition-to-revenue.md) | Join Parquet and billing at the correct grain |
| [4. Gameplay and support](04-gameplay-and-support.md) | ClickHouse aggregation, CRM joins, and runtime key restrictions |
| [5. Channel health across four domains](05-channel-health.md) | Combine summaries without multiplying raw facts |
| [6. Acceleration: cache a three-source answer](06-acceleration.md) | What is stored and how the original SELECT becomes a local cached read |
| [7. Automatic substitution](07-automatic-substitution.md) | A cached three-source summary joined to live revenue, with ORDER BY and LIMIT |
| [8. Upstream writes and refresh](08-refresh-and-freshness.md) | Insert real CRM rows, observe stale results, bypass the cache, and catch up |
| [9. Accelerator measurements](09-accelerator-measurements.md) | Actual cold/warm runtimes, the warm regression, and the full reproducible experiment |
| [10. Results and data contracts](10-data-contracts.md) | What the plans establish and the limits of this synthetic dataset |
| [11. When substitution does not apply](11-when-substitution-does-not-apply.md) | Seven real refusal cases, two positive controls, and freshness/latency limits |

## Running and extending the showcase

- [Setup: Docker sources and FedQ server](setup.md)
- [Inspect plans, fragments, and verify answers](inspection-and-verification.md)
- [Additional substitution correctness checks](substitution-checks.md)
- [Use the CLI, Python, or a notebook](cli-and-notebooks.md)
- [Reset and extend the dataset](reset-and-extend.md)

The [join substitution evidence](evidence/substitution-revenue.json) records
the cached-support/live-revenue execution. The [accelerator evidence](evidence/accelerator.json) retains the measured
lifecycle runs. [Substitution evidence](evidence/substitution.json) retains
the correctness checks and actual execution fragments.
[Boundary evidence](evidence/substitution-boundaries.json) records the refusal cases.
