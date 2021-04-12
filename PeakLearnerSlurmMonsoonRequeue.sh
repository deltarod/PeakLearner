#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/home/tem83/PeakLearner/runlog.txt
#SBATCH --chdir=/home/tem83/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --ntasks=1
#SBATCH --mem=2
#SBATCH --time=10:00

module load anaconda3
module load R
conda activate PLVenv
srun python3 Slurm/run.py monsoon
