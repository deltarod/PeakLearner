#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/home/tristan/Research/PeakLearner/runlog.txt
#SBATCH --chdir=/home/tristan/Research/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --ntasks=1
#SBATCH --time=1:00:00

source /home/tristan/anaconda3/bin/activate PLVenv

srun python3 Slurm/run.py monsoon
