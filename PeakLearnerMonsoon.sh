#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/scratch/tem83/runlog.txt
#SBATCH --chdir=/home/tem83/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --ntasks=1
#SBATCH --time=1:00:00

module load anaconda3
conda activate PeakLearner
module load R

srun python3 Slurm/run.py monsoon

#sbatch PeakLearnerMonsoon.sh
