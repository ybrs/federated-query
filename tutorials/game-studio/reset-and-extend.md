# Reset and extend the dataset

Stop FedQ server with Ctrl-C in its terminal first. Then remove this example's
containers, volumes, and generated files, and generate and load again.
These commands target only this tutorial:

```bash
docker compose -f tutorials/game-studio/compose.yaml down -v
rm -rf tutorials/game-studio/generated
docker compose -f tutorials/game-studio/compose.yaml up -d --wait
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/generate.py --players 1000 --seed 42
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/seed.py
```

Restart FedQ server with the command in [the setup chapter](setup.md), then run:

```bash
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/verify.py
```

`--players` changes the registered population and generates the same number
of additional anonymous visitors. `--seed` changes the synthetic journeys.
Generation holds the fixture in memory; it is intended for tutorial datasets.

Natural follow-up exercises are subscription changes with effective dates,
refunds with signed amounts, ticket-to-event links, channel spend by day,
and a fixed-window retention measure with equal follow-up for each cohort.
Each needs an explicit data contract before it can answer the corresponding
business question reliably.

---

[Contents](README.md) | [Previous: cli-and-notebooks.md](cli-and-notebooks.md)
