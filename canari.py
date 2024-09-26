from math import ceil
from datetime import datetime
import os.path
import socket
import argparse
import glob
import sys
import os
import re
import json
import subprocess
import heapq
import logging
import shlex
import random
import time
from pathlib import Path

from typing import Union, Optional
from uuid import uuid4

###      Global Variables Begin      ###
health_check_apps = ['hpl', 'stream']  #Applications comprising a health check
hostname=socket.gethostname()
old_style=hostname.split('-')
cluster = old_style[0] if len(old_style) > 1 else hostname.split('.')[1]

#Directory that this script(that you are reading right now) is located in. Use to reference relative files
repo_dir:str = os.path.dirname(__file__)

###       Global Variables End       ###
### Cluster Specific Variables Begin ###

### Cluster Specific Variables End ###


#################################  CLUSTER HEALTH CODE ##########################################
class Node:
    """
    Class represents a node along with information about why its offlined.
    Note: This class overrides the python default methods:
        - __str__()
        - __repr__()
        - __lt__()
    The first 2 cases are for convenience methods for debugging, the
    third is used to provide a custom comparator for the purposes of sorting. 
    The heapq library checks this method when comparing
    non primitives against one another.

    """
    def __init__(self, name, reason, user, timestamp):
        """
        Initializes a node with fields from sinfo.
        """
        self.name = name
        self.reason = reason
        self.user = user
        self.timestamp = timestamp

    def get_name(self):
        """
        Returns the hostname of the node.
        """
        return self.name

    def get_datetime(self):
        """
        Returns the timestamp as a Datetime Object.
        """
        return datetime.strptime(self.timestamp, '%Y-%m-%dT%H:%M:%S')

    def __str__(self):
        """
        Overridden method provides the hostname of the node instead of the
        default address + class mangling
        """
        return self.get_name()

    def __repr__(self):
        """
        Overridden method provides the hostname of the node instead of the
        default address + class mangling.
        Note: There is a nuanced difference between this and __str__(). It is
        begign in my use case.
        """
        return self.__str__()

    def __lt__(self, other):
        """
        Overridden comparator allowing the use of sort on a list of nodes.
        """
        return self.timestamp < other.timestamp


def decouple_nodes(node_name:str, debug=False) -> list:
    """
    Function reformats nodelists formatted as a[xxx-yyy], a[xxx,xxy-yyz],
    or a[xxx,xxy-yyz],c[www,wwv,vv-u] to individually list them instead.

    node_name: str 
        Examples:   'a001' --> ['a001']
                    'c[001,005,010-012]' --> ['c001', 'c005', 'c010', 'c011', 'c012']
                    'bell-a299' --> ['bell-a299']
                    'bell-a[223,321]' --> ['bell-a223', 'bell-a321']
                    'bell-a[220,225-229]' --> ['bell-a220', 'bell-a225', 'bell-a226', 'bell-a227', 'bell-a228', 'bell-a229']
    """
    if node_name.find('[') == -1: #Just a single node - does not need recoupling
        return node_name.split(",") #Something like bell-a117,bell-b004 is a possibility
    decoupled_nodes=[]
    node_range_regex=re.compile(r"\[[\d\,\-]+\]")
    bracket_idxs=re.finditer(node_range_regex, node_name)
    for bracket_idx in bracket_idxs:
        node_type= node_name[:bracket_idx.start()] #node_name[bracket_idx.start() - 1]
        node_idxs=node_name[bracket_idx.start() + 1 : bracket_idx.end() - 1]
        node_ranges=node_idxs.split(",")
        #At this point a string like a[xxx,xxy-yyz] should be a python list with elements xxx, xxy-xxz
        for node_range in node_ranges:
            regexp = re.compile(r'[0-9]-[0-9]')
            if not regexp.search(node_range): #If it's a single node add it back
                for node_idx in node_range.split(","):
                    decoupled_nodes.append(node_type + node_idx)
            else:
                limits=node_range.split("-")
                for idx in range(int(limits[0]),int(limits[1])+1):
                    decoupled_nodes.append(node_type + str(idx).zfill(3))
    return decoupled_nodes

def get_offline_node_objs() -> list[Node]:
    """ 
    Returns a list of Node objects that contains the information output in sinfo -R.
    """
    output, _ = run_bash_command("sinfo -Rh -o %100E~%9u~%19H~%N")

    ''' output should look like this:
    NHC: check_fs_free:  /tmp has only 0% free (36kB), minimum is 3%                                    ~root     ~2024-09-18T20:02:30~b005
    NHC: Terminated by signal SIGTERM.                                                                  ~root     ~2024-09-25T09:55:30~c[001,005,010-012]
    NHC: check_dmi_data_match:  No match found for BIOS Information: Version: 2.9.4                     ~root     ~2024-09-25T09:25:25~c007
    testing AMD GPUs in k8s                                                                             ~goughes  ~2024-09-24T15:28:17~g001
    '''
    nodelist=[]
    # Get last element of every line
    relevant_strings = [[l.strip() for l in line.split("~")] for line in output.split('\n') if len(line.split()) > 0] 
    ''' relevant strings should look like this:
    [['NHC: Terminated by signal SIGTERM.', 'root', '2024-09-25T10:05:30', 'c[000,003-004]'],
    ['NHC: check_dmi_data_match:  No match found for BIOS Information: Version: 2.9.4', 'root','2024-09-25T09:25:25','c007'],
    ['NHC: Terminated by signal SIGTERM.', 'root', '2024-09-25T09:55:30','c007'],
    ['testing AMD GPUs in k8s', 'goughes', '2024-09-24T15:28:17', 'g001']]
    '''

    for s in relevant_strings:
        for decoupled_node in decouple_nodes(s[3], debug=True): #Pulls out the "node" string (c[000,003-004], c007, c007, g001)
            nodelist.append(Node(decoupled_node, s[0], s[1], s[2])) #Create a node object and add to nodelist
    #Different string formats are:
    #   1. aXXX
    #   2. a[XXX-YYY]
    #   3. a[XXX-YYY, ZZZ, ...]
    #decouple_nodes([n.name for n in nodelist], debug=True)
    return sorted(nodelist, key=lambda x: datetime.strptime(x.timestamp, '%Y-%m-%dT%H:%M:%S'))


def get_total_nodes(partition = "testpbs" ) -> int:
    """
    Helper function returning an integer representing the total number of nodes.
    """
    command_string= f"sinfo -s | grep {partition} | tr -s \" \" | cut -d \" \" -f4 | cut -d \"/\" -f4"
    total_nodes, error = run_bash_command(command_string, return_error=True)
    return int(total_nodes)


def log_status(filepath='/depot/itap/verburgt/repos/cluster_health/cluster_health_data/', offline_nodes=None, keep=168):
    """
    A function which logs the currently offlined_nodes to the directory specified by filepath.
    If a list of Nodes is not passed as offlined_nodes, log_status will re-run that function to
    retrieve them. This function also deletes the oldest log file once the filepath contains "keep"
    number of files. By default this is 24*7=168 files.
    """
    if not os.path.exists(filepath):
        Path(filepath).mkdir(parents=True, exist_ok=True)
    files=glob.glob(os.path.join(filepath , '*'))
    if len(files) >= keep:
        os.remove(min(files, key=os.path.getctime)) # Removing only one (oldest) ?? - Potential bug if somehow more than one files get added (JV)
    if not offline_nodes:
        offline_nodes=get_offline_node_objs() # List of Node objects
    json_dump=json.dumps([node.__dict__ for node in offline_nodes])
    with open(os.path.join(filepath, 
                           str(datetime.now().strftime("%Y-%m-%d-%H%M%S"))), "w+") as f:
        f.write(json_dump)


def get_last_healthcheck(filepath='/depot/itap/verburgt/repos/cluster_health/cluster_health_data/'):
    """
    Helper function which retrieves the log the last healthcheck and returns
    the data contained within as a list of Node objects. If the last healthcheck
    ran more than 90 minutes ago, then the data is stale and we should record this
    healthcheck without notification of changes. In this case, we return None.
    """
    files=glob.glob(os.path.join(filepath , '*'))
    if len(files) == 0:
        return []
    last_file=max(files, key=os.path.getctime)
    last_created_time = datetime.strptime(time.ctime(os.path.getctime(last_file)), '%c')
    current_time = datetime.now()
    delta = current_time - last_created_time
    recent_healthcheck = True
    if delta.seconds > 60*90:
        recent_healthcheck = False
        return None
    with open(last_file, "r") as f:
        content=f.read()
        return json.loads(content, object_hook=lambda d: Node(**d))

################################# CLUSTER_BENCHMARKING CODE ##########################################

def get_all_nodes(partition = "testpbs") -> list[str]:
    output, _ = run_bash_command(f'sinfo -sa | grep {partition} | tr -s " "| cut -d " " -f5')
    # output looks like:  'bell-a[000-479],bell-b[000-007,011]\n'

    #split on commas not in brackets
    coupled_nnodes = re.split(r',\s*(?![^\[]*\])', output.strip()) #Should be cluster agnostic.
    decoupled_nodes = []
    for coupled_node in coupled_nnodes:
        single_nodes = decouple_nodes(coupled_node)
        decoupled_nodes.extend(single_nodes)

    # decoupled nodes looks like ['bell-a000','bell-a001','bell-a002', 'bell-a003', ..., 'bell-b007','bell-b011']
    return decoupled_nodes

def run_bash_command(command, return_error = False):
    """
    Runs the specified Bash command and returns the output of stdout. If
    return_error is specified as True, it will also return the output of
    stderr in the format stdout, stderr. Will function with pipelines and
    respect spaces within quoted substrings.
    """
    commands=command.split("|")
    p1=None
    p2=None
    logging.debug(f"Command string seen by 'run_bash_command' is {command}")
    for command in commands:
        if p1 == None: #First command in pipeline
            p1=subprocess.Popen(shlex.split(command), stdout = subprocess.PIPE)
            if len(commands) == 1: #No pipelines exists
                p2=p1
            continue
        else:
            p2=subprocess.Popen(shlex.split(command), stdin = p1.stdout, stdout = subprocess.PIPE)
        p1.stdout.close() #type:ignore
        p1=p2

    output, error = p2.communicate() #type:ignore
    output = output.decode('ascii')
    if return_error:
        return output, error
    else:
        return output, None

def get_offline_nodes() -> set[str]:
    """ 
    Returns a list of strings that contains the names of nodes that are offline
    which should be avoided.
    """
    output, _ = run_bash_command("sinfo -R")
    output_lines = output.split('\n')
    offline_nodes=[]

    # Get last element of every line
    relevant_strings = [line.split()[-1] for line in output_lines if len(line.split()) > 0] 
    offline_nodes = []
    for s in relevant_strings:
        logging.debug(f"s is {s}")
        for offline_node in decouple_nodes(s):
            offline_nodes.append(s)

    #Different string formats are:
    #   1. aXXX
    #   2. a[XXX-YYY]
    #   3. a[XXX-YYY, ZZZ, ...]
    
    return set(offline_nodes)
 
def submit_slurm(subfile:str, 
                 outfile:Optional[str] = None,
                 dependent_job:Optional[Union[int,str]] = None, 
                 host:Optional[str] = None, 
                 export_vars:Optional[list] = None, 
                 gpus:Optional[int] = None,
                 account:Optional[str] = None,
                 partition:Optional[str] = None,
                 constraint:Optional[str] = None,
                 return_command = False) -> Union[int, str]:
        '''Submits a slurm job and returns the Job ID'''

        #Build up the command based on what arguments are expected
        command = "sbatch --parsable "
        if host is not None:
             command += f"-w {host} "
        if gpus is not None:
            command += f"-G {gpus} "
        if outfile is not None:
            command += f"-o {outfile} "
        if dependent_job is not None:
             command += f"--dependency=afterany:{dependent_job} "
        if account is not None:
            command += f"--account={account} "
        if partition is not None:
            command += f"--partition={partition} "
        if constraint is not None:
            command += f"--constraint={constraint.upper()} "
        if export_vars is not None:
            command += f"--export={','.join(export_vars)} "
        command += subfile

        #Just return the command string if requested
        if return_command:
            return command

        #Submit the command
        logging.debug(f"submit_slurm submitting command {command}")
        job=subprocess.run(shlex.split(command), capture_output = True, text = True)
        if job.returncode == 0:
            job_id = int(str(job.stdout).strip())
            return job_id
        else:
             logging.error("Unable to submit slurm job")
             logging.error(job.stderr)
             raise RuntimeError(f"Unable to submit slurm job")
        

def get_submission_script(application:str, 
                          device:str, 
                          node_type:str, 
                          benchmarking_apps_path:str = os.path.join(repo_dir, "benchmarking_apps")) -> str:   #TODO  don't hardcode this
    match application:
        case "hpl":
            submission_script = os.path.join(benchmarking_apps_path,cluster,"hpl",device,"submit","node_type",node_type,"submit.sh" )
        case "stream":
            submission_script = os.path.join(benchmarking_apps_path,cluster,"stream", "submit", "submit.sh" )
            # submission_script = os.path.join(root_dir,cluster,"stream", "submit","node_type",node_type,"submit.sh" )
        case _:
            raise NotImplementedError(f"application must be 'hpl' or 'stream'. Given value is '{application}'")
    return submission_script


def get_database_update_script(application:str, 
                               timestamp:str, 
                               benchmarking_outfile:str,
                               device:str,
                               database_login_file:str, 
                               env_dir:str,
                               jobid:Optional[Union[int,str]] = None,
                               script_path = os.path.join(repo_dir,"update_db/update_db.sh")                               ):
    
    if not jobid:
        jobid = ""
    submission_script = f"{script_path} {cluster} {application} {timestamp} {benchmarking_outfile} {device} {database_login_file} {jobid} {repo_dir} {env_dir}"
    return submission_script


def get_flagjob_script(benchmarking_command,
                       logging_command,
                       script_path = os.path.join(repo_dir, "slurm", "flag_job.sh")):
    submission_script = f"{script_path} '{benchmarking_command}' '{logging_command}'"
    return submission_script

def submit_benchmarking_pair(application:str,
                             database_login_file:str,
                             host:Optional[str] = None,
                             device:str="cpu", #cpu/gpu 
                             working_dir:str = os.path.join(repo_dir, "testing"), #Directory to write files to - Must have write access
                             env_dir:str = os.path.join(repo_dir, ".conda","envs", "canari"),
                             benchmarking_apps_path = os.path.join(repo_dir, "benchmarking_apps"),
                             node_type:Optional[str] = None,
                             account:Optional[str] = None,
                             partition:Optional[str] = None,
                             flag_job:Optional[bool] = False, 
                             gpu_required:bool = False) -> None:

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S") #TODO Remove! "Update_db" will pull start time from sacct
    uuid = uuid4()
    #Submit Benchmarking script

    if node_type is None:
        #If Node type is None, the host MUST be set (so we can get the node from it)
        assert host
        node_type = get_nodetype_from_host(host)
        #node_type = "a"

    ###################### Submit everything in a flag job if required ###################### 
    if flag_job and application == "stream": 
        '''The flag job runs a dummy job with a random wait time to implicitly 
        stop dependent jobs from running at the same time'''

        #Get command for submitting the benchmaring job
        submission_script = get_submission_script(application, device, node_type, benchmarking_apps_path = benchmarking_apps_path)
        benchmarking_outfile = os.path.join(working_dir, f'benchmark_{timestamp}_{cluster}_{application}_{uuid}')
        benchmarking_command = submit_slurm(submission_script, 
                                          host = host,
                                          outfile = benchmarking_outfile, 
                                          account=account, 
                                          partition=partition, 
                                          constraint = node_type, 
                                          return_command = True)

        #Get comand for logging the output
        database_update_script = get_database_update_script(application = application, 
                                                            timestamp = timestamp,  #TODO Remove! "Update_db" will pull start time from sacct
                                                            benchmarking_outfile=benchmarking_outfile,
                                                            device=device,
                                                            database_login_file=database_login_file, 
                                                            jobid="JOBID_PLACEHOLDER", 
                                                            env_dir = env_dir) 
        
        required_gpus = 1 if gpu_required else None
        logging_outfile = os.path.join(working_dir, f'dbupdate_{timestamp}_{cluster}_{application}_{uuid}')
        logging_command = submit_slurm(database_update_script, 
                                       outfile= logging_outfile,  
                                       gpus=required_gpus, 
                                       account=account, 
                                       partition=partition, 
                                       return_command = True,
                                       dependent_job="JOBID_PLACEHOLDER")


        flag_script = get_flagjob_script(benchmarking_command = benchmarking_command,
                                         logging_command = logging_command)
  
        flag_outfile = os.path.join(working_dir, f'flag_{timestamp}_{cluster}_{application}_{uuid}')
        flag_jobid = submit_slurm(flag_script, host = host, outfile=flag_outfile, account=account, partition=partition)


        
    else:  #Submit normally
        ###################### Submit the benchmarking job ###################### 
        submission_script = get_submission_script(application, device, node_type, benchmarking_apps_path = benchmarking_apps_path)
        benchmarking_outfile = os.path.join(working_dir, f'benchmark_{timestamp}_{cluster}_{application}_{uuid}')
        benchmarking_jobid = submit_slurm(submission_script, host = host, outfile= benchmarking_outfile, account=account, partition=partition, constraint = node_type)
        logging.debug(f"Benchmarking Job ID is {benchmarking_jobid}. Writing to {benchmarking_outfile}")

        ###################### Submit dependent database update script ###################### 
        #TODO this is a mess, clean this up
        database_update_script = get_database_update_script(application = application, 
                                                            timestamp = timestamp,  #TODO Remove! "Update_db" will pull start time from sacct
                                                            benchmarking_outfile=benchmarking_outfile,
                                                            device=device,
                                                            database_login_file=database_login_file, 
                                                            jobid=benchmarking_jobid, 
                                                            env_dir = env_dir)
        #Gilbreth strictly requires GPUs
        required_gpus = 1 if gpu_required else None
        logging_outfile = os.path.join(working_dir, f'dbupdate_{timestamp}_{cluster}_{application}_{uuid}')
        logging_jobid = submit_slurm(database_update_script, outfile= logging_outfile, 
                                    dependent_job=benchmarking_jobid, gpus=required_gpus, 
                                    account=account, partition=partition)
        logging.debug(f"Logging Job ID is {logging_jobid}. Writing to {logging_outfile}")


def get_cluster_data(cluster:str, 
                     json_path:Optional[str] = os.path.join(repo_dir,"cluster_data.json") ) -> dict:
    '''Fetches data about the cluster currently being ran
    
    cluster:str = "bell", 
    json_path:str = "cluster_data.json"
    returns: dict
    '''
    with open(json_path, 'r') as file: #type:ignore
        # Load the JSON data from the file
        data = json.load(file)
    try:
        return data[cluster]
    except KeyError as e:
        logging.error(f"Unable to fetch data for cluster {cluster} ")
        raise e

def get_nodetype_from_host(host: str) -> str:
    '''Takes a host ("bell-a135", "a076") and returns the nodetype ('a')'''
    logging.debug(f"extracting node type from host name {host}")
    match = re.search(r'(?:-|\b)(\w)\d', host)
    if match:
        logging.debug(f"extracted node type is {match.group(1)}")
        return match.group(1)
    else:
        raise RuntimeError("Unable to extract node from host")


def node_split(node_count:dict[str,int], num_nodes:int, weighted:bool = True) -> dict[str,int]:

    total_node_count = sum(node_count.values())
    total_node_type_count = len(node_count)


    jobs_on_node:dict[str,int] = {}
    if weighted:
        for node, count in node_count.items():
            nobs_for_this_node = round(count/total_node_count * num_nodes)
            jobs_on_node[node] = nobs_for_this_node
    else:
        for node, count in node_count.items():
            nobs_for_this_node = round(num_nodes / total_node_type_count)
            jobs_on_node[node] = nobs_for_this_node
            
    return jobs_on_node



def split_node_fraction(node_type:Union[str, list[str], None],
                        num_nodes:int, 
                        nodes:dict, 
                        default_node:str = "a", weighted = True) -> dict[str, int]:

    #If no node is provided, use the default node (Pulled from cluster_data.json)
    if node_type is None:
        node_type = [default_node]

    #If it was provided, make sure it is a list
    elif isinstance(node_type, str):
        if node_type == "all":  #Use all nodes if the string was "all"
            node_type = list(nodes.keys())
        else: #Otherwise convert the string to a list
            node_type = [n for n in node_type]

    #Make all nodes lowercase
    node_type = [n.lower() for n in node_type]

    #Make sure that all the node types are available on this cluster.
    assert all([ n in nodes.keys() for n in node_type]) #["a", "b"]

    #Get counts for each node type
    node_count = {node:nodes[node]["count"] for node in node_type} #{'a': 480, 'b': 8}
    
    #Split based on the fraction
    jobs_on_nodetype = node_split(node_count, num_nodes, weighted = weighted)
    return jobs_on_nodetype


def main():
    #Set up argument parser
    parser = argparse.ArgumentParser(description='Provides a framework for streamlining application verification.')

    parser.add_argument('--res', metavar='reservation_name', type=str,
            help='Used when launching jobs under a maintenance for which a reservation has been created.\n')
    parser.add_argument('--process', metavar='process', type=str, choices=['cpu', 'gpu'], default = "cpu",
            help='Used to set whether the CPU is used or the GPU.')
    parser.add_argument("-l", '--log_level', type=str, 
            choices=['debug', 'info', "warning", "error", "critical"], default = "warning", help='Set the logging level')
    
    
    #Benchmarking options
    parser.add_argument("--flag_job", action = "store_true", default = False, help="Add a flag job to space out STREAM job submissions")
    parser.add_argument("--node_type", type=str, nargs = "+", default = None)
    parser.add_argument("--no_weight", action="store_false", help = "Will evenly distribute jobs across nodes if passed. Distribition is determined by node counts by default")
    host_options=parser.add_mutually_exclusive_group(required=False)
    host_options.add_argument('--hosts', metavar='hostnames', type=str, nargs='+',
            help='A list of hostnames on which to run. Pass "all" to run on all online nodes\n')
    host_options.add_argument('--num_nodes', metavar='N', type=int, default = None,
            help='A number of nodes on which to run the programs. These nodes will be randomly sampled.\n')
    
    # Cluster health options
    parser.add_argument("--health_log", action = "store_true", default = False, help="Run a healthcheck")

    #Parse arguments and act accordingly
    args = parser.parse_args()

    #Set Logging
    log_level_info = {'DEBUG': logging.DEBUG, 
                      'INFO': logging.INFO,
                      'WARNING': logging.WARNING,
                      'ERROR': logging.ERROR,
                      'CRITICAL':logging.CRITICAL
                     }
    log_level = log_level_info.get(args.log_level.upper(), logging.INFO) 
    logging.basicConfig(level=log_level, 
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    try:
        assert (bool(args.hosts or args.num_nodes) ^ args.health_log)
    except AssertionError as e:
        raise Exception("Either args.hosts or args.num_nodes must be set for benchmarking, or health_log for cluster health checking")


    cluster_data = get_cluster_data(cluster=cluster)
    primary_node = cluster_data["primary_node"]
    database_login_file = cluster_data["database_login_file"]
    nodes = cluster_data["nodes"]
    device  = nodes[primary_node]["devices"][0] #TODO Deal with nodes with both CPU and GPU
    account = cluster_data["account"]
    partition = cluster_data["partition"]
    gpu_required = cluster_data["gpu_required"]
    benchmarking_apps_path = cluster_data["benchmarking_apps_path"] #Path to where submission files are located
    health_data_dir = cluster_data["health_data_dir"]
    env_dir = cluster_data["env_dir"]
    working_dir = cluster_data["working_dir"]
    os.makedirs(working_dir, exist_ok = True)


    offline_nodes=get_offline_nodes()
    hosts = []
    logging.info(f"Hostname is {hostname}")
    logging.info(f"Cluster is {cluster}")
    logging.debug(f"Offline Nodes are {' '.join(offline_nodes)}")
    all_nodes=get_all_nodes(partition = partition)
    logging.debug(f"All Nodes are {' '.join(all_nodes)}")
    available_nodes = [node for node in all_nodes if node not in offline_nodes]

    if args.num_nodes:
        node_split = split_node_fraction(node_type = args.node_type, 
                                         num_nodes = args.num_nodes,
                                         nodes = nodes,
                                         default_node = primary_node,
                                         weighted = args.no_weight) #Returns a dictionary of node_type:number of nodes to submit
        
        for node_type, num in  node_split.items():
            for _ in range(num):
                for app in health_check_apps:   #  ["stream"]: #
                    submit_benchmarking_pair(application = app, 
                                            host = None,
                                            device = device,
                                            database_login_file=database_login_file, 
                                            node_type = node_type, 
                                            account=account,
                                            partition=partition, 
                                            benchmarking_apps_path = benchmarking_apps_path, 
                                            working_dir = working_dir, 
                                            flag_job = args.flag_job, 
                                            gpu_required = gpu_required, 
                                            env_dir = env_dir)
        
    elif args.hosts:
        if args.hosts[0] == "all":
            logging.info(f"Running for all nodes on {cluster}")
            args.hosts = available_nodes
        for app in health_check_apps:
            for host in args.hosts:
                submit_benchmarking_pair(application = app, 
                                            host = host,
                                            device = device,
                                            database_login_file=database_login_file, 
                                            node_type = None, 
                                            account=account,
                                            partition=partition, 
                                            benchmarking_apps_path = benchmarking_apps_path, 
                                            working_dir = working_dir, 
                                            flag_job = args.flag_job,
                                            gpu_required = gpu_required, 
                                            env_dir = env_dir)

    elif args.health_log:
        log_status(filepath = health_data_dir)
    else:
        raise NotImplementedError("Either --hosts or --num_nodes or --health_log most be provided!")

if __name__ == "__main__":
    main()