#!/bin/bash

module load python/anaconda-2022.05 spark/3.3.2

pyspark \
  --total-executor-cores 4 \
  --executor-memory 8G \
  --driver-memory 8G