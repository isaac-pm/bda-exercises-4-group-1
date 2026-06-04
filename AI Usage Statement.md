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
  - OpenAI GPT-5.5 thinking

Q1: Is the planned financial-risk pipeline correct for Problem 3?

```
Asked for validation of the planned approach: three stock time series, three
market-factor time series, temporal alignment, 10-trading-day returns, factor
features, one regression model per stock, Monte Carlo simulation, and VaR/CVaR
reporting.
```

A1:

```
The approach is consistent with the RunMonteCarlo methodology. The important
points are to parse the CSV files with a Spark/RDD stage, align all six series
on shared dates, compute two-week returns with a 10-trading-day lag, use
r, r^2, and sqrt(abs(r)) as factor features, and compute 95% VaR/CVaR from the
lower tail of simulated returns.
```

Q2: Any advice on environment setup and CSV compatibility?

```
Asked how to keep the local setup compatible with the course workflow, including
the BigData virtual environment, PySpark execution, StockAnalysis/Yahoo-style
CSV files, and Date/Close parsing.
```

A2:

```
The suggested setup was to run source ~/venvs/BigData/bin/activate before local
execution, verify the Python/PySpark versions, keep the CSV files under
data/problem_3, parse Date and Close robustly, and store the script under
solutions/problem_3.
```

Q3: Any suggestions for checking the Monte Carlo run?

```
Asked for advice on validating the implementation and output before relying on
the final 1,000,000-trial experiment.
```

A3:

```
The advice was to check that all six CSV files are loaded, verify that the
aligned row count is reasonable, confirm that the return row count is aligned
rows minus the 10-day lag, and verify that VaR/CVaR values are generated.
```

Q4: How should the final results be interpreted and reported?

```
Asked for advice on reporting portfolio VaR/CVaR, individual stock VaR/CVaR,
and the comparison between correlated and independent market-factor sampling.
```

A4:

```
The guidance was to include the selected assets, data locations, aligned date
range, row counts, simulation settings, Spark runtimes, portfolio VaR/CVaR,
per-stock VaR/CVaR, and a short interpretation of best/worst stock based mainly
on downside risk.
```
