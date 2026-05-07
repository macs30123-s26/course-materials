#!/bin/bash

#SBATCH --job-name=spark-example
#SBATCH --output=spark.out
#SBATCH --error=spark.err
#SBATCH --nodes=1
#SBATCH --ntasks=10
#SBATCH --mem=40G
#SBATCH --partition=caslake
#SBATCH --account=macs30123

module load python/anaconda-2022.05 spark/3.3.2

export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3
export PYSPARK_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3

spark-submit --total-executor-cores 9 --executor-memory 4G --driver-memory 4G exercise/lab_wk7_spark.py