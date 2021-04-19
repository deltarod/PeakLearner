#!/bin/bash
#SBATCH --job-name=PLSlurm
#SBATCH --output=/scratch/tem83/runlog.txt
#SBATCH --chdir=/home/tem83/PeakLearner/
#SBATCH --open-mode=append
#SBATCH --ntasks=1
#SBATCH --mem=2048
#SBATCH --time=15:00
#SBATCH --export=ALL

module load R
module load anaconda3
conda activate PeakLearner

/home/tem83/.conda/envs/PeakLearner/bin/python Slurm/run.py monsoon

sbatch PeakLearnerMonsoon.sh
