#!/usr/bin/env python3
"""Assignment 4 Problem 3: Monte Carlo financial risk analysis."""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError
from datetime import date, datetime, timezone
from functools import reduce
from pathlib import Path

import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import avg, col, lag, lit, pow, sqrt
from pyspark.sql.types import DateType, DoubleType, StructField, StructType
from pyspark.sql.window import Window


STOCKS = [
    {"name": "AAPL", "label": "Apple Inc.", "symbol": "AAPL"},
    {"name": "MSFT", "label": "Microsoft Corp.", "symbol": "MSFT"},
    {"name": "NVDA", "label": "NVIDIA Corp.", "symbol": "NVDA"},
]

FACTORS = [
    {"name": "SP500", "label": "S&P 500", "symbol": "^GSPC"},
    {"name": "NASDAQ100", "label": "Nasdaq 100", "symbol": "^NDX"},
    {"name": "DOWJONES", "label": "Dow Jones Industrial Average", "symbol": "^DJI"},
]

DATE_FORMATS = ["%Y-%m-%d", "%b %d, %Y", "%d-%b-%y", "%m/%d/%Y"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data/problem_3", help="Problem 3 data directory")
    parser.add_argument("--download-data", action="store_true", help="Download/refresh Yahoo Finance CSV files")
    parser.add_argument("--start-date", default="2021-06-04", help="Download start date, yyyy-mm-dd")
    parser.add_argument("--end-date", default="2026-06-04", help="Download end date, yyyy-mm-dd")
    parser.add_argument("--n-trials", type=int, default=1_000_000, help="Monte Carlo trials")
    parser.add_argument("--n-partitions", type=int, default=10, help="Spark partitions for simulations")
    parser.add_argument("--seed", type=int, default=42, help="Base random seed")
    parser.add_argument("--quantile-error", type=float, default=0.0, help="Spark approxQuantile relative error")
    parser.add_argument(
        "--output-json",
        default="solutions/problem_3/problem_3_results.json",
        help="Path for machine-readable results",
    )
    return parser.parse_args()


def parse_date(value: str) -> date | None:
    cleaned = value.strip().strip('"')
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            pass
    return None


def clean_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().strip('"').replace(",", "").replace("$", "")
    if not cleaned or cleaned.lower() in {"null", "none", "nan"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def yahoo_chart_url(symbol: str, start: date, end: date) -> str:
    period1 = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    period2 = int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp())
    encoded = urllib.parse.quote(symbol, safe="")
    return (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        f"?period1={period1}&period2={period2}&interval=1d&events=history"
    )


def download_yahoo_csv(asset: dict[str, str], target: Path, start: date, end: date) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    url = yahoo_chart_url(asset["symbol"], start, end)
    payload = fetch_json(url)

    chart = payload.get("chart", {})
    if chart.get("error"):
        raise RuntimeError(f"Yahoo chart error for {asset['symbol']}: {chart['error']}")

    result = chart.get("result") or []
    if not result:
        raise RuntimeError(f"No Yahoo chart result for {asset['symbol']}")

    result0 = result[0]
    timestamps = result0.get("timestamp") or []
    quote = (result0.get("indicators", {}).get("quote") or [{}])[0]
    adjclose = (result0.get("indicators", {}).get("adjclose") or [{}])[0].get("adjclose") or []

    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
        for i, ts in enumerate(timestamps):
            close_value = (quote.get("close") or [None] * len(timestamps))[i]
            if close_value is None:
                continue
            day = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            writer.writerow(
                [
                    day,
                    value_at(quote, "open", i),
                    value_at(quote, "high", i),
                    value_at(quote, "low", i),
                    close_value,
                    adjclose[i] if i < len(adjclose) else close_value,
                    value_at(quote, "volume", i),
                ]
            )


def fetch_json(url: str) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }
    last_error: Exception | None = None
    for curl_binary in ("curl.exe", "curl"):
        try:
            completed = subprocess.run(
                [curl_binary, "-L", "-A", headers["User-Agent"], "-sS", url],
                check=True,
                capture_output=True,
                text=True,
            )
            return json.loads(completed.stdout)
        except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError) as exc:
            last_error = exc

    for attempt in range(4):
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 502, 503, 504} or attempt == 3:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch JSON: {url}") from last_error


def value_at(container: dict[str, list], key: str, index: int):
    values = container.get(key) or []
    if index >= len(values):
        return ""
    return "" if values[index] is None else values[index]


def download_all_data(data_dir: Path, start: date, end: date) -> dict[str, str]:
    for asset in STOCKS:
        target = data_dir / "stocks" / f"{asset['name']}.csv"
        download_yahoo_csv(asset, target, start, end)
    for asset in FACTORS:
        target = data_dir / "factors" / f"{asset['name']}.csv"
        download_yahoo_csv(asset, target, start, end)
    return source_urls(start, end)


def source_urls(start: date, end: date) -> dict[str, str]:
    sources = {}
    for asset in STOCKS:
        sources[asset["name"]] = yahoo_chart_url(asset["symbol"], start, end)
    for asset in FACTORS:
        sources[asset["name"]] = yahoo_chart_url(asset["symbol"], start, end)
    return sources


def parse_csv_row(line: str, date_idx: int, close_idx: int) -> Row | None:
    try:
        parts = next(csv.reader([line]))
    except csv.Error:
        return None
    if max(date_idx, close_idx) >= len(parts):
        return None
    parsed_date = parse_date(parts[date_idx])
    close_value = clean_float(parts[close_idx])
    if parsed_date is None or close_value is None:
        return None
    return Row(date=parsed_date, close=close_value)


def find_header_indices(header: str) -> tuple[int, int]:
    columns = [c.strip().strip('"').lower() for c in next(csv.reader([header]))]
    try:
        date_idx = columns.index("date")
    except ValueError as exc:
        raise ValueError(f"CSV header is missing a Date column: {header}") from exc

    close_candidates = ["close", "adj close", "adjclose", "adjusted close"]
    for candidate in close_candidates:
        if candidate in columns:
            return date_idx, columns.index(candidate)
    raise ValueError(f"CSV header is missing a Close column: {header}")


def load_price_series(spark: SparkSession, path: Path, name: str):
    if not path.exists():
        raise FileNotFoundError(f"Missing required data file for {name}: {path}")

    text_rdd = spark.sparkContext.textFile(str(path))
    header = text_rdd.first()
    date_idx, close_idx = find_header_indices(header)
    parsed_rdd = (
        text_rdd.zipWithIndex()
        .filter(lambda pair: pair[1] > 0)
        .map(lambda pair: parse_csv_row(pair[0], date_idx, close_idx))
        .filter(lambda row: row is not None)
    )

    schema = StructType(
        [
            StructField("date", DateType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
        ]
    )
    df = spark.createDataFrame(parsed_rdd, schema=schema).dropna().orderBy("date")
    stats = df.agg(
        lit(name).alias("name"),
        avg("close").alias("mean_close"),
    ).first()
    count = df.count()
    min_max = df.selectExpr("min(date) as min_date", "max(date) as max_date").first()
    return df, {
        "rows": count,
        "min_date": str(min_max["min_date"]),
        "max_date": str(min_max["max_date"]),
        "mean_close": float(stats["mean_close"]),
        "path": str(path),
    }


def load_all_series(spark: SparkSession, data_dir: Path):
    series = {}
    stats = {}
    for asset in STOCKS:
        df, item_stats = load_price_series(spark, data_dir / "stocks" / f"{asset['name']}.csv", asset["name"])
        series[asset["name"]] = df
        stats[asset["name"]] = item_stats
    for asset in FACTORS:
        df, item_stats = load_price_series(spark, data_dir / "factors" / f"{asset['name']}.csv", asset["name"])
        series[asset["name"]] = df
        stats[asset["name"]] = item_stats
    return series, stats


def align_series(series: dict[str, object]):
    min_dates = []
    max_dates = []
    for df in series.values():
        min_max = df.selectExpr("min(date) as min_date", "max(date) as max_date").first()
        min_dates.append(min_max["min_date"])
        max_dates.append(min_max["max_date"])

    global_start = max(min_dates)
    global_end = min(max_dates)

    aligned_parts = []
    for name, df in series.items():
        trimmed = df.filter((col("date") >= global_start) & (col("date") <= global_end))
        aligned_parts.append(trimmed.withColumnRenamed("close", name))

    aligned = reduce(lambda left, right: left.join(right, on="date", how="inner"), aligned_parts).orderBy("date")
    aligned_count = aligned.count()
    return aligned, {"start": str(global_start), "end": str(global_end), "rows": aligned_count}


def compute_returns(aligned_df):
    window_spec = Window.orderBy("date")
    result = aligned_df
    for asset in [a["name"] for a in STOCKS + FACTORS]:
        result = result.withColumn(f"{asset}_return", (col(asset) / lag(col(asset), 10).over(window_spec)) - 1.0)
    return_cols = ["date"] + [f"{asset['name']}_return" for asset in STOCKS + FACTORS]
    return result.select(*return_cols).dropna().orderBy("date")


def add_factor_features(df, factor_cols: list[str], output_col: str = "features"):
    transformed = df
    feature_cols = []
    for factor_col in factor_cols:
        squared_col = f"{factor_col}_squared"
        sqrt_abs_col = f"{factor_col}_sqrt_abs"
        transformed = transformed.withColumn(squared_col, pow(col(factor_col), 2.0)).withColumn(
            sqrt_abs_col, sqrt(spark_abs(col(factor_col)))
        )
        feature_cols.extend([factor_col, squared_col, sqrt_abs_col])

    assembler = VectorAssembler(inputCols=feature_cols, outputCol=output_col)
    return assembler.transform(transformed)


def train_models(returns_df, factor_cols: list[str], stock_cols: list[str]):
    featurized = add_factor_features(returns_df, factor_cols).select("date", "features")
    training_df = returns_df.select("date", *stock_cols).join(featurized, on="date", how="inner").cache()
    training_count = training_df.count()

    models = {}
    model_stats = {}
    for stock_col in stock_cols:
        data = training_df.select("features", col(stock_col).alias("label")).dropna()
        model = LinearRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=0.0).fit(data)
        model.setPredictionCol(f"{stock_col}_pred")
        models[stock_col] = model
        model_stats[stock_col] = {
            "intercept": float(model.intercept),
            "coefficients": [float(x) for x in model.coefficients.toArray().tolist()],
            "rmse": float(model.summary.rootMeanSquaredError),
            "r2": float(model.summary.r2),
        }

    training_df.unpersist()
    return models, model_stats, training_count


def factor_stats(returns_df, factor_cols: list[str]) -> tuple[np.ndarray, np.ndarray, dict[str, object]]:
    rows = returns_df.select(*factor_cols).collect()
    matrix = np.array([[float(row[c]) for c in factor_cols] for row in rows], dtype=float)
    mu = matrix.mean(axis=0)
    covariance = np.cov(matrix, rowvar=False)
    return mu, covariance, {
        "factor_columns": factor_cols,
        "means": {factor_cols[i]: float(mu[i]) for i in range(len(factor_cols))},
        "covariance": covariance.tolist(),
        "stddevs": {factor_cols[i]: float(math.sqrt(covariance[i, i])) for i in range(len(factor_cols))},
    }


def trials_for_partition(total: int, partitions: int, partition_index: int) -> int:
    base = total // partitions
    remainder = total % partitions
    return base + (1 if partition_index < remainder else 0)


def sampled_factor_df(
    spark: SparkSession,
    factor_cols: list[str],
    mu: np.ndarray,
    covariance: np.ndarray,
    n_trials: int,
    n_partitions: int,
    seed: int,
    independent: bool,
):
    stddevs = np.sqrt(np.diag(covariance))
    schema = StructType([StructField(c, DoubleType(), nullable=False) for c in factor_cols])

    def generate(partition_index: int):
        count = trials_for_partition(n_trials, n_partitions, partition_index)
        rng = np.random.default_rng(seed + partition_index)
        if independent:
            samples = rng.normal(loc=mu, scale=stddevs, size=(count, len(mu)))
        else:
            samples = rng.multivariate_normal(mean=mu, cov=covariance, size=count)
        for sample in samples:
            yield Row(**{factor_cols[i]: float(sample[i]) for i in range(len(factor_cols))})

    rdd = spark.sparkContext.parallelize(range(n_partitions), n_partitions).flatMap(generate)
    return spark.createDataFrame(rdd, schema=schema)


def predict_returns(sampled_factors, factor_cols: list[str], stock_cols: list[str], models: dict[str, object]):
    prediction_df = add_factor_features(sampled_factors, factor_cols).select("features")
    for stock_col in stock_cols:
        prediction_df = models[stock_col].transform(prediction_df)

    pred_cols = [f"{stock_col}_pred" for stock_col in stock_cols]
    portfolio_expr = sum(col(c) for c in pred_cols) / float(len(pred_cols))
    return prediction_df.withColumn("portfolio_return_pred", portfolio_expr).select(*pred_cols, "portfolio_return_pred")


def risk_metrics(df, column_name: str, quantile_error: float) -> dict[str, float]:
    var_95 = float(df.approxQuantile(column_name, [0.05], quantile_error)[0])
    cvar_95 = float(df.filter(col(column_name) <= var_95).select(avg(column_name)).first()[0])
    mean_return = float(df.select(avg(column_name)).first()[0])
    return {"mean": mean_return, "var_95": var_95, "cvar_95": cvar_95}


def run_simulation(
    spark: SparkSession,
    label: str,
    factor_cols: list[str],
    stock_cols: list[str],
    models: dict[str, object],
    mu: np.ndarray,
    covariance: np.ndarray,
    args: argparse.Namespace,
    independent: bool,
):
    start = time.perf_counter()
    sampled = sampled_factor_df(
        spark,
        factor_cols,
        mu,
        covariance,
        args.n_trials,
        args.n_partitions,
        args.seed + (10_000 if independent else 0),
        independent=independent,
    )
    predicted = predict_returns(sampled, factor_cols, stock_cols, models).cache()
    simulation_count = predicted.count()
    risks = {"portfolio_return_pred": risk_metrics(predicted, "portfolio_return_pred", args.quantile_error)}
    for stock_col in stock_cols:
        pred_col = f"{stock_col}_pred"
        risks[pred_col] = risk_metrics(predicted, pred_col, args.quantile_error)
    predicted.unpersist()
    elapsed = time.perf_counter() - start
    return {
        "label": label,
        "independent_factors": independent,
        "trials": simulation_count,
        "risk_metrics": risks,
        "runtime_seconds": elapsed,
    }


def choose_best_worst(correlated_result: dict[str, object], stock_cols: list[str]) -> dict[str, object]:
    stock_risks = correlated_result["risk_metrics"]
    ranked = sorted(
        stock_cols,
        key=lambda stock_col: (
            stock_risks[f"{stock_col}_pred"]["cvar_95"],
            stock_risks[f"{stock_col}_pred"]["var_95"],
            stock_risks[f"{stock_col}_pred"]["mean"],
        ),
    )
    worst = ranked[0]
    best = ranked[-1]
    return {
        "best_stock": best.replace("_return", ""),
        "worst_stock": worst.replace("_return", ""),
        "criterion": "Ranked primarily by less negative/more negative 95% CVaR, then VaR and mean return.",
    }


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    total_start = time.perf_counter()
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if start is None or end is None:
        raise ValueError("--start-date and --end-date must use a supported date format")
    sources = source_urls(start, end)
    if args.download_data:
        sources = download_all_data(data_dir, start, end)

    spark = (
        SparkSession.builder.appName("Assignment4Problem3MonteCarlo")
        .config("spark.sql.shuffle.partitions", str(args.n_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    timings: dict[str, float] = {}
    try:
        section_start = time.perf_counter()
        series, series_stats = load_all_series(spark, data_dir)
        timings["load_parse_seconds"] = time.perf_counter() - section_start

        section_start = time.perf_counter()
        aligned_df, alignment_stats = align_series(series)
        returns_df = compute_returns(aligned_df).cache()
        return_rows = returns_df.count()
        returns_minmax = returns_df.selectExpr("min(date) as min_date", "max(date) as max_date").first()
        timings["alignment_returns_seconds"] = time.perf_counter() - section_start

        stock_cols = [f"{asset['name']}_return" for asset in STOCKS]
        factor_cols = [f"{asset['name']}_return" for asset in FACTORS]

        section_start = time.perf_counter()
        models, model_stats, training_rows = train_models(returns_df, factor_cols, stock_cols)
        mu, covariance, factor_summary = factor_stats(returns_df, factor_cols)
        timings["model_training_seconds"] = time.perf_counter() - section_start

        correlated = run_simulation(
            spark,
            "correlated_market_factors",
            factor_cols,
            stock_cols,
            models,
            mu,
            covariance,
            args,
            independent=False,
        )
        timings["correlated_simulation_seconds"] = correlated["runtime_seconds"]

        independent = run_simulation(
            spark,
            "independent_market_factors",
            factor_cols,
            stock_cols,
            models,
            mu,
            covariance,
            args,
            independent=True,
        )
        timings["independent_simulation_seconds"] = independent["runtime_seconds"]
        timings["total_seconds"] = time.perf_counter() - total_start

        result = {
            "portfolio": STOCKS,
            "factors": FACTORS,
            "data_source": "Yahoo Finance chart API daily historical prices; equivalent public CSV history source.",
            "download_sources": sources,
            "data_dir": str(data_dir),
            "series_stats": series_stats,
            "alignment": alignment_stats,
            "returns": {
                "rows": return_rows,
                "start": str(returns_minmax["min_date"]),
                "end": str(returns_minmax["max_date"]),
                "lag_trading_days": 10,
            },
            "training_rows": training_rows,
            "factor_summary": factor_summary,
            "model_stats": model_stats,
            "monte_carlo": {
                "n_trials": args.n_trials,
                "n_partitions": args.n_partitions,
                "seed": args.seed,
                "portfolio_weights": "Equal weights, 1/3 each stock.",
                "quantile_error": args.quantile_error,
                "correlated": correlated,
                "independent": independent,
            },
            "stock_interpretation": choose_best_worst(correlated, stock_cols),
            "timings": timings,
            "environment": {
                "python": f"{__import__('sys').version_info.major}.{__import__('sys').version_info.minor}.{__import__('sys').version_info.micro}",
                "spark": spark.version,
            },
        }

        output_json.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
