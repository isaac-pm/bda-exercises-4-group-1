#!/usr/bin/env python3
"""
RunTaxiTrips.py — NYC Taxi Trip Analysis (PySpark RDD API)

Usage:
    python RunTaxiTrips.py [data_dir]

    data_dir  path containing trip_data-part*.csv and
              nyc-borough-boundaries-polygon.geojson.json
              (default: ../../data/problem_2)

Environment:
    SPARK_DRIVER_MEMORY   JVM heap for the driver (default: 200g)
    SPARK_CORES           parallelism, e.g. 4 or * (default: *)
"""

import os
import sys
import json
import logging
import math
import time
from collections import namedtuple
from datetime import datetime

# ── Driver memory must be set BEFORE the JVM starts ──────────────────────────
DRIVER_MEM = os.environ.get("SPARK_DRIVER_MEMORY", "200g")
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {DRIVER_MEM} pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.statcounter import StatCounter
from shapely.geometry import Point, shape

# ── Logging: every record is flushed immediately ──────────────────────────────


class _FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


class _FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"taxi_{_ts}.log")

_fmt = logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_stdout_h = _FlushHandler(sys.stdout)
_stdout_h.setFormatter(_fmt)
_file_h = _FlushFileHandler(_log_path, mode="w")
_file_h.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_stdout_h, _file_h])
log = logging.getLogger(__name__)


def _sep(title=""):
    log.info("")
    log.info("─" * 72)
    if title:
        log.info(f"  {title}")
        log.info("─" * 72)


log.info(f"Log file:      {_log_path}")
log.info(f"Driver memory: {DRIVER_MEM}")

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "problem_2"
    )
)
DATA_DIR = os.path.abspath(DATA_DIR)
log.info(f"Data dir:      {DATA_DIR}")

TRIP_GLOB = os.path.join(DATA_DIR, "trip_data-part*.csv")
GEOJSON = os.path.join(DATA_DIR, "nyc-borough-boundaries-polygon.geojson.json")

for path in [GEOJSON]:
    if not os.path.exists(path):
        log.error(f"Required file not found: {path}")
        sys.exit(1)

# ── Spark ─────────────────────────────────────────────────────────────────────

CORES = os.environ.get("SPARK_CORES", "*")
spark = (
    SparkSession.builder.appName("RunTaxiTrips")
    .master(f"local[{CORES}]")
    .config("spark.driver.memory", DRIVER_MEM)
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")
log.info(f"Spark {sc.version}  parallelism={sc.defaultParallelism}")

# ── Parsing helpers ───────────────────────────────────────────────────────────

TaxiTrip = namedtuple("TaxiTrip", ["pickupTime", "dropoffTime", "pickupLoc", "dropoffLoc"])


def parse(line: str):
    f = line.split(",")
    return (
        f[1],
        TaxiTrip(
            datetime.strptime(f[5], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(f[6], "%Y-%m-%d %H:%M:%S"),
            (float(f[10]), float(f[11])),
            (float(f[12]), float(f[13])),
        ),
    )


def safe(fn):
    def _w(s):
        try:
            return fn(s)
        except Exception as e:
            return (s, e)

    return _w


def get_duration_seconds(trip):
    return (trip.dropoffTime - trip.pickupTime).total_seconds()


def get_duration_hours(trip):
    return int(get_duration_seconds(trip) / 3600)


def haversine_km(loc1, loc2):
    lon1, lat1 = loc1
    lon2, lat2 = loc2
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))


def normalized_duration(trip):
    dist = haversine_km(trip.pickupLoc, trip.dropoffLoc)
    return None if dist == 0.0 else get_duration_seconds(trip) / dist


TripRecord = namedtuple(
    "TripRecord",
    [
        "license",
        "pickup_ts",
        "dropoff_ts",
        "pickup_hour",
        "pickup_boro",
        "dropoff_boro",
        "dur_sec",
        "norm_dur",
    ],
)

# ── Borough polygon lookup ────────────────────────────────────────────────────

_sep("Borough polygons")
with open(GEOJSON) as f:
    _geojson = json.load(f)

bFeatures = sc.broadcast(_geojson["features"])
log.info(f"Broadcast {len(_geojson['features'])} polygons")


def _borough(lon, lat):
    p = Point(lon, lat)
    for feat in bFeatures.value:
        if p.within(shape(feat["geometry"])):
            return feat["properties"]["borough"]
    return None


def pickup_borough(trip):
    return _borough(*trip.pickupLoc)


def dropoff_borough(trip):
    return _borough(*trip.dropoffLoc)


# ── Load & enrich ─────────────────────────────────────────────────────────────

_sep("Loading and enriching trip data (borough + metrics pre-computed)")
t0 = time.time()

taxiDone = (
    spark.read.text(TRIP_GLOB).rdd.map(lambda row: safe(parse)(row.value))
    .filter(lambda x: not isinstance(x[1], Exception))
    .filter(lambda x: 0 <= get_duration_hours(x[1]) < 3)
    .filter(lambda x: x[1].pickupLoc != (0.0, 0.0) and x[1].dropoffLoc != (0.0, 0.0))
    .map(
        lambda x: TripRecord(
            license=x[0],
            pickup_ts=x[1].pickupTime.timestamp(),
            dropoff_ts=x[1].dropoffTime.timestamp(),
            pickup_hour=x[1].pickupTime.hour,
            pickup_boro=pickup_borough(x[1]),
            dropoff_boro=dropoff_borough(x[1]),
            dur_sec=get_duration_seconds(x[1]),
            norm_dur=normalized_duration(x[1]),
        )
    )
)
taxiDone.cache()

n_trips = taxiDone.count()
log.info(f"Clean trips cached: {n_trips:,}  ({time.time() - t0:.0f}s)")

# ── (a) Same-borough trips ────────────────────────────────────────────────────

_sep("(a) Trips starting and ending in the SAME borough")
t0 = time.time()

same_boro = (
    taxiDone.filter(
        lambda r: r.pickup_boro and r.dropoff_boro and r.pickup_boro == r.dropoff_boro
    )
    .map(lambda r: (r.pickup_boro, r.dur_sec))
    .mapValues(lambda d: StatCounter().merge(d))
    .reduceByKey(lambda a, b: a.mergeStats(b))
    .collect()
)

log.info(f"  {'Borough':15s}  {'Count':>8s}  {'Sum (s)':>16s}  {'Mean (s)':>9s}")
for boro, st in sorted(same_boro, key=lambda x: x[0] or ""):
    log.info(
        f"  {str(boro):15s}  {int(st.count()):>8,}  {st.sum():>16,.0f}  {st.mean():>9.1f}"
    )
log.info(f"Elapsed: {time.time() - t0:.0f}s")

# ── (b) Cross-borough trips ───────────────────────────────────────────────────

_sep("(b) Trips starting and ending in DIFFERENT boroughs")
t0 = time.time()

cross_boro = (
    taxiDone.filter(
        lambda r: r.pickup_boro and r.dropoff_boro and r.pickup_boro != r.dropoff_boro
    )
    .map(lambda r: ((r.pickup_boro, r.dropoff_boro), r.dur_sec))
    .mapValues(lambda d: StatCounter().merge(d))
    .reduceByKey(lambda a, b: a.mergeStats(b))
    .collect()
)

log.info(
    f"  {'Origin':15s}   {'Destination':15s}  {'Count':>7s}  {'Sum (s)':>14s}  {'Mean (s)':>9s}"
)
for (pb, db), st in sorted(cross_boro, key=lambda x: x[0]):
    log.info(
        f"  {pb:15s} -> {db:15s}  {int(st.count()):>7,}  {st.sum():>14,.0f}  {st.mean():>9.1f}"
    )
log.info(f"Elapsed: {time.time() - t0:.0f}s")

# ── Sessionization ────────────────────────────────────────────────────────────

_sep("Sessionization — 4-hour gap split")
t0 = time.time()


def secondaryKey(r):
    return r.pickup_ts


def split(r1, r2):
    return (r2.pickup_ts - r1.pickup_ts) / 3600 >= 4


def groupSorted(it, splitFn):
    cur_lic, cur_sess = None, []
    for key, value in it:
        lic = key[0]
        if cur_lic is None:
            cur_lic, cur_sess = lic, [value]
        elif lic != cur_lic or splitFn(cur_sess[-1], value):
            yield (cur_lic, cur_sess)
            cur_lic, cur_sess = lic, [value]
        else:
            cur_sess.append(value)
    if cur_sess:
        yield (cur_lic, cur_sess)


NUM_PARTS = max(sc.defaultParallelism * 4, 200)

sessions = (
    taxiDone.map(lambda r: (r.license, r))
    .map(lambda x: ((x[0], secondaryKey(x[1])), x[1]))
    .partitionBy(NUM_PARTS)
    .sortByKey()
    .mapPartitions(lambda it: groupSorted(it, split))
)
sessions.cache()

n_sess = sessions.count()
log.info(f"Total sessions: {n_sess:,}  ({time.time() - t0:.0f}s)")

# ── (c) Wait-time per (borough, hour-of-day) ──────────────────────────────────

_sep("(c) Average wait-time between consecutive trips per borough and hour-of-day")
t0 = time.time()


def boro_hour_wait(r1, r2):
    return ((r1.pickup_boro, r1.pickup_hour), r2.pickup_ts - r1.dropoff_ts)


bh_stats = (
    sessions.values()
    .flatMap(
        lambda trips: (boro_hour_wait(trips[i], trips[i + 1]) for i in range(len(trips) - 1))
    )
    .filter(lambda x: x[1] >= 0)
    .mapValues(lambda d: StatCounter().merge(d))
    .reduceByKey(lambda a, b: a.mergeStats(b))
    .collect()
)

log.info(f"  {'Borough':15s}  {'Hour':>4s}  {'Count':>6s}  {'Mean (s)':>10s}")
for (boro, hour), st in sorted(bh_stats, key=lambda x: (x[0][0] or "", x[0][1])):
    log.info(
        f"  {str(boro):15s}  {hour:>4d}  {int(st.count()):>6,}  {st.mean():>10.1f}"
    )
log.info(f"Elapsed: {time.time() - t0:.0f}s")

# ── (d) Outlier detection — 95th-percentile normalised duration ───────────────

_sep("(d) Outlier detection — 95th-percentile normalised duration per borough")
t0 = time.time()

norm_rdd = (
    taxiDone.filter(lambda r: r.pickup_boro is not None and r.norm_dur is not None)
    .map(lambda r: (r.pickup_boro, r.norm_dur))
)
norm_rdd.cache()

thresholds = (
    norm_rdd.groupByKey()
    .mapValues(lambda vals: sorted(vals))
    .mapValues(lambda sv: sv[int(0.95 * len(sv))])
    .collectAsMap()
)

log.info(f"  {'Borough':15s}  {'95th-pct (s/km)':>16s}")
for b, thr in sorted(thresholds.items(), key=lambda x: x[0] or ""):
    log.info(f"  {str(b):15s}  {thr:>16.2f}")

thr_bc = sc.broadcast(thresholds)
outlier_counts = norm_rdd.filter(
    lambda x: x[1] > thr_bc.value.get(x[0], float("inf"))
).countByKey()

log.info(f"  {'Borough':15s}  {'Outliers':>10s}")
for b, cnt in sorted(outlier_counts.items(), key=lambda x: x[0] or ""):
    log.info(f"  {str(b):15s}  {cnt:>10,}")
log.info(f"Elapsed: {time.time() - t0:.0f}s")

norm_rdd.unpersist()

# ── (e) Rush-hour detection ───────────────────────────────────────────────────

_sep("(e) Rush-hour detection — avg normalised duration per hour (descending)")
t0 = time.time()

rush = (
    taxiDone.filter(lambda r: r.norm_dur is not None)
    .map(lambda r: (r.pickup_hour, r.norm_dur))
    .mapValues(lambda d: StatCounter().merge(d))
    .reduceByKey(lambda a, b: a.mergeStats(b))
    .map(lambda x: (x[0], x[1].mean()))
    .collect()
)
rush.sort(key=lambda x: -x[1])

log.info(f"  {'Hour':>4s}  {'Avg norm dur (s/km)':>20s}")
for hour, mean_nd in rush:
    log.info(f"  {hour:>4d}  {mean_nd:>20.2f}")
log.info(f"Elapsed: {time.time() - t0:.0f}s")

# ── Done ──────────────────────────────────────────────────────────────────────

_sep("All tasks complete")
spark.stop()
log.info("Spark stopped.")
