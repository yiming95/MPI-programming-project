#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=0-12:00:00

# Load required modules
module load Python/3.5.2-intel-2016.u3

# Launch multiple process python code
echo "1 node 1 core big twitter dataset"
time mpirun -np 1 python ccc1.py /home/yimingz8/melbGrid.json /home/yimingz8/bigTwitter.json


