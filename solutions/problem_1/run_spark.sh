#!/bin/bash
#SBATCH --job-name=marvel_graph
#SBATCH --nodes=2
#SBATCH --time=01:00:00

module purge
module load devel/Spark/3.5.4-foss-2023b-Java-17
module load lang/Python/3.11.5-GCCcore-13.2.0

spark-submit --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 \
                --master yarn \
                Exercise1_Marvel_GraphAnalysis.ipynb
