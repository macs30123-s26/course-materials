#!/bin/bash
#SBATCH --job-name=pycuda_reduction
#SBATCH --output=pycuda_reduction.out
#SBATCH --time=00:05:00      # cancels job after 5 minutes of wall time
#SBATCH --partition=gpu      # GPU partition
#SBATCH --nodes=1            # 1 GPU node
#SBATCH --ntasks=1           # 1 CPU core to drive GPU
#SBATCH --gres=gpu:1         # Request 1 GPU
#SBATCH --constraint=rtx6000 # specify GPU model; alternatives: v100, a100
#SBATCH --account=macs30123

module load python/anaconda-2022.05 cuda/11.7 gcc/10.2.0

python pycuda_reduction.py
