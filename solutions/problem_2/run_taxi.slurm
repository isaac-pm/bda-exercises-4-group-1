#!/bin/bash
#SBATCH --job-name=bda-taxi
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err
#SBATCH --partition=batch

# ── Strict mode ───────────────────────────────────────────────────────────────
set -eo pipefail

# System profile scripts may have unbound vars — disable nounset temporarily
set +u
if [ -f /etc/profile.d/modules.sh ]; then
    source /etc/profile.d/modules.sh
fi
set -u

# ── Conda ─────────────────────────────────────────────────────────────────────
CONDA_BASE="${HOME}/miniforge3"

if [[ ! -f "${CONDA_BASE}/etc/profile.d/conda.sh" ]]; then
    echo "ERROR: Conda not found at ${CONDA_BASE}. See setup.txt."
    exit 1
fi

source "${CONDA_BASE}/etc/profile.d/conda.sh"
conda activate bda-taxi

# Java home = conda env prefix (openjdk installed there via conda)
export JAVA_HOME="${CONDA_PREFIX}"
export PATH="${JAVA_HOME}/bin:${PATH}"

# ── Spark settings ────────────────────────────────────────────────────────────
# Leave ~16 GB for OS/kernel; give the rest to the JVM driver
TOTAL_MEM_GB=$(free -g | awk '/^Mem:/{print $2}')
DRIVER_MEM_GB=$(( TOTAL_MEM_GB - 16 ))
export SPARK_DRIVER_MEMORY="${DRIVER_MEM_GB}g"
export SPARK_CORES="*"          # use all available cores
export PYSPARK_PYTHON="$(which python)"

# ── Data path — edit if your dataset lives elsewhere ─────────────────────────
DATA_DIR="${HOME}/bda-exercises-4-group-1/data/problem_2"

# ── Diagnostics ───────────────────────────────────────────────────────────────
echo "======================================================================"
echo "  BDA Exercise 4 — Problem 2 — NYC Taxi Trip Analysis"
echo "======================================================================"
echo "Job ID:        ${SLURM_JOB_ID}"
echo "Node:          $(hostname)"
echo "Cores:         $(nproc)"
echo "Total RAM:     ${TOTAL_MEM_GB} GB"
echo "Driver memory: ${SPARK_DRIVER_MEMORY}"
echo "Python:        ${PYSPARK_PYTHON}"
echo "Java:          $(java -version 2>&1 | head -1)"
echo "Data dir:      ${DATA_DIR}"
echo "Submit dir:    ${SLURM_SUBMIT_DIR:-$PWD}"
echo "======================================================================"
echo ""

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [[ ! -d "${DATA_DIR}" ]]; then
    echo "ERROR: DATA_DIR not found: ${DATA_DIR}"
    echo "Edit the DATA_DIR variable in this script or see setup.txt."
    exit 1
fi

if [[ $(ls "${DATA_DIR}"/trip_data-part*.csv 2>/dev/null | wc -l) -eq 0 ]]; then
    echo "ERROR: No trip_data-part*.csv files found in ${DATA_DIR}"
    exit 1
fi

SCRIPT="$(dirname "$(realpath "$0")")/RunTaxiTrips.py"
if [[ ! -f "${SCRIPT}" ]]; then
    echo "ERROR: RunTaxiTrips.py not found at ${SCRIPT}"
    exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────
cd "${SLURM_SUBMIT_DIR:-$(dirname "${SCRIPT}")}"

python "${SCRIPT}" "${DATA_DIR}"
