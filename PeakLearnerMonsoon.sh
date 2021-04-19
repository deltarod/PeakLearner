#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/home/tristan/Research/PeakLearner/runlog.txt
#SBATCH --chdir=/home/tristan/Research/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --ntasks=1
#SBATCH --time=1:00:00

module anaconda3
source /home/tristan/anaconda3/bin/activate PLVenv
module R

srun python3 Slurm/run.py monsoon

sbatch -Q eakLearnerMonsoon.sh
