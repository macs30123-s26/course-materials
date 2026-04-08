#!/bin/bash
#SBATCH --job-name=gpu_activity
#SBATCH --output=gpu_activity.out
#SBATCH --partition=gpu
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:1
#SBATCH --constraint=rtx6000
#SBATCH --account=macs30123

module load python/anaconda-2022.05 cuda/11.7 gcc/10.2.0

python activity-cuda.py
#python activity-opencl.py
