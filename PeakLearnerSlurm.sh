#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/home/tmiller/PeakLearner/runlog.txt
#SBATCH --chdir=/home/tmiller/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --time=1:01:00

srun python3 Slurm/run.py
