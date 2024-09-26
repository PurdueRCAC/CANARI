#!/bin/bash
#SBATCH --time=05:00
#SBATCH --nodes=1
#SBATCH --job-name="update_db"
#SBATCH --tasks-per-node=1
#SBATCH --account=rcac
#SBATCH --partition=benchmarking
#SBATCH --export="NIL . NONE"

#Set positional input arguments
#Why can't bash support keyword arguments :(
cluster=${1}
application=${2}
datetime=${3} #TODO REMOVE! No longer used. Python script will pull from Job ID
outfile=${4}
device=${5}
database_login_file=${6}
jobid=${7}

repo_dir=${8}
env_dir=${9}

#Turn on correct environment 
module --force purge
module load anaconda
conda init

# case $RCAC_CLUSTER in
#   gilbreth)
#     repo_dir=/depot/itap/verburgt/repos/cluster_benchmarking/
#     env_dir=/depot/itap/verburgt/envs/anaconda/cluster_benchmarking
#     ;;
#   bell)
#     repo_dir=/depot/itap/verburgt/repos/cluster_benchmarking/
#     env_dir=/depot/itap/verburgt/envs/anaconda/cluster_benchmarking
#     ;;
#   negishi)
#     repo_dir=/depot/itap/verburgt/repos/cluster_benchmarking/
#     env_dir=/depot/itap/verburgt/envs/anaconda/cluster_benchmarking
#     ;;
#   anvil)
#     repo_dir=/home/x-jverburgt/cluster_benchmarking/
#     env_dir=/home/x-jverburgt/.conda/envs/2024.02-py311/cluster_benchmarking
#     ;;
#   *)
#     echo "Operating on an unrecognized cluster ($RCAC_CLUSTER) Exiting"
#     exit 1
#     ;;
# esac

echo "Activating correct conda env"
conda activate $env_dir
echo "Python exec is $(which python )"


#Move to correct directory
cd $SLURM_SUBMIT_DIR
echo "Running in ${PWD}"


python ${repo_dir}/update_db/update_db.py\
    --cluster ${cluster}\
    --application ${application}\
    --outfile ${outfile}\
    --device ${device}\
    --database_login_file ${database_login_file}\
    --jobid ${jobid}\
    --log_level="debug"\


