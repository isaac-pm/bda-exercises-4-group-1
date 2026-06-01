# Problem 1

- Models used:
  - XXX

QX:

```

```

AX:

```

```

---

# Problem 2

- Models used:
  - Claude Sonnet 4.6 (via Claude Code CLI)

Q1: Is the overall structure of the solution correct for the five tasks?

```
Reviewed the approach for tasks (a)–(e): borough grouping, sessionization logic,
normalised-duration metric, and 95th-percentile threshold computation.
```

A1:

```
The structure looks correct. StatCounter with reduceByKey is the right pattern
for aggregating trip durations. The Haversine formula is appropriate for
straight-line distance normalisation. The 95th-percentile threshold can be
computed by collecting normalised durations per borough, sorting, and indexing
at int(0.95 * n).
```

Q2: Any suggestions for running the full dataset on the HPC cluster?

```
Asked for advice on packaging the solution as a standalone Python script
suitable for SLURM batch submission on Aion.
```

A2:

```
Suggested structuring the script so that driver memory is configured via an
environment variable before the JVM starts, and that all results are logged
incrementally rather than only at the end, so partial output is preserved
if the job is interrupted.
```

---

# Problem 3

- Models used:
  - XXX

QX:

```

```

AX:

```

```
