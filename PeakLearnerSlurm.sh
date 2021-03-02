#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/home/tristan/Research/PeakLearner/runlog.txt
#SBATCH --chdir=/home/tristan/Research/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --time=1:15

srun python3 Slurm/run.py
