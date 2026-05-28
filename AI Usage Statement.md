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

Q1: Generate a PySpark notebook for NYC taxi trip analysis (problems a–e) based on
the reference RunTaxiTrips.ipynb. Fix the borough lookup bug (reference uses pickupLoc
for both pickup and dropoff). Add Haversine distance for normalisation in (d)/(e).
Apply day-13 filter before caching to avoid OOM on local hardware.

```
Problem 2 requires: (a) same-borough trip stats, (b) cross-borough stats,
(c) wait-time per (borough, hour), (d) 95th-percentile outlier detection,
(e) rush-hour ranking by normalised duration. Base it on the reference notebook
at solutions/problem_2/references/RunTaxiTrips.ipynb. Do not run anything —
write the solution notebook and instruct me to run it.
```

A1:

```
Claude produced solutions/problem_2/RunTaxiTrips.ipynb covering all five tasks.
Key design decisions:
- borough_for_loc(lon, lat) replaces the buggy reference that used pickupLoc for
  both ends; separate pickup_borough/dropoff_borough helpers used for (a)/(b).
- Haversine formula (pure math, no external geo library) for distance in (d)/(e).
- Day-13 filter applied before .cache() to keep JVM heap usage manageable locally.
- StatCounter + reduceByKey used throughout as required by the assignment.
- groupByKey() used for (d) 95th-percentile: collects normalised durations per
  borough, sorts, indexes at int(0.95 * n), broadcasts thresholds back.
```

Q2: The initial notebook OOM-crashed because taxiParsed.cache() tried to hold all
14M rows before filtering. Fix it.

```
The full taxiParsed RDD was being cached before the day-13 filter was applied,
causing Java heap OOM. Move the filter before cache() and eliminate intermediate
cached RDDs (taxiAll, taxiGood, taxiParsed).
```

A2:

```
Restructured cell-04-load as a single pipeline: read → safe(parse) → filter
(not Exception) → filter (day == 13) → filter (duration) → filter (coordinates)
→ cache(). Removed taxiAll, taxiGood, taxiParsed intermediate caches and the
redundant second data load that was in cell-08. Result: only ~434k rows cached.
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
