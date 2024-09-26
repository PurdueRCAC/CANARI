#!/bin/bash
#SBATCH --time=40:00
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --job-name="flag_job"
#SBATCH --account="testpbs"

# This script is a wrapper
benchmarking_command=$1
logging_command=$2



echo "benchmarking_command:$benchmarking_command"
echo "logging_command:$logging_command"


echo "SLURM_JOB_USER: $SLURM_JOB_USER"
echo "SLURM_JOBID: $SLURM_JOBID"
echo "SLURM_SUBMIT_DIR: $SLURM_SUBMIT_DIR"
echo "SLURM_JOB_NAME: $SLURM_JOB_NAME"
echo "SLURM_CLUSTER_NAME: $SLURM_CLUSTER_NAME"
echo "SLURM_SUBMIT_HOST: $SLURM_SUBMIT_HOST"

echo "date: $(date)"
echo "SLURM_JOB_START_TIME: $SLURM_JOB_START_TIME"
echo "SLURM_JOB_END_TIME: $SLURM_JOB_END_TIME"

echo "SLURM_NODELIST: $SLURM_NODELIST"
echo "SLURM_NNODES: $SLURM_NNODES"
echo "SLURM_NPROCS: $SLURM_NPROCS"
echo "SLURM_NTASKS: $SLURM_NTASKS"
echo "SLURM_MEM_PER_CPU: $SLURM_MEM_PER_CPU"


min=10
max=30
sleep_minutes=$((min+RANDOM%(max-min))).$((RANDOM%999))
#sleep_minutes=0.1 # for testing

echo "Sleeping for ${sleep_minutes} minutes!"
sleep ${sleep_minutes}m
#start job here

echo "Submitting benchmarking job at $(date)"
benchmarking_jobid=$($benchmarking_command)
echo "Benchmarking job ID is ${benchmarking_jobid}"
# logging_command= "$logging_command -"
# logging_jobid=$($logging_command)


# Replace JOBID_PLACEHOLDER with benchmarking_jobid
logging_command="${logging_command//JOBID_PLACEHOLDER/$benchmarking_jobid}"
echo "patched logging command is $logging_command"
logging_jobid=$($logging_command)

#TODO Don't exit until benchmarking_jobID has started
sleep 10m
echo "Done Sleeping! - Exiting"

