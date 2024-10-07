# CANARI: A Monitoring Framework for Cluster Analysis and Node Assessment for Resource Integrity

## Description

CANARI (Cluster Analysis and Node Assessment for Resource Integrity) provides a framework for monitoring and reporting the health and performance of compute nodes in a Slurm based HPC cluster. The intent is to provide real-time diagnostics and alerts to cluster maintainers. CANARI primarily works by interfacing with SLURM and backend nodes directly to collect diagnostic data, and reports this data to cluster maintainers via a Slack API. In practice, these tools provide can bring attention to events affecting node availability and performance that may otherwise go unnoticed.

CANARI can be broken down into two main functionalities:

* Cluster Benchmarking 

    Cluster Benchmarking routinely runs two benchmarking applications [STREAM](https://www.cs.virginia.edu/stream/) and [HPL](https://www.netlib.org/benchmark/hpl/index.html), and logs the results in a Postgres database. To collect data, "pairs" of jobs are submitted to backend nodes. These consist of a benchmarking job that run the benchmarking application, and a second job that is dependent upon the first that will read the results of the benchmark into a Postgres database. For HPL, the GFlops are reported, and for STREAM, the memory bandwidth rate for the triad operation is reported.

    For simplicity, all benchmarking data is stored in a single table titled ```benchmark_results_simple```, and keeps track of the cluster, node, device, benchmarking application, and application performance over time:


    | result_id | cluster | node       | device | application | performance | datetime            |
    |-----------|---------|------------|--------|-------------|-------------|---------------------|
    | 63455     | bell    | bell-a319  | cpu    | stream      | 218699.2    | 2024-09-19 12:00:38 |
    | 63445     | bell    | bell-a055  | cpu    | hpl         | 1411.4      | 2024-09-19 10:38:36 |
    | 63444     | bell    | bell-a082  | cpu    | stream      | 217194.5    | 2024-09-19 10:38:36 |
    | 63439     | bell    | bell-a343  | cpu    | stream      | 219057.3    | 2024-09-19 00:00:14 |
    | 63443     | bell    | bell-a345  | cpu    | stream      | 203218.8    | 2024-09-19 00:00:14 |
    | 63429     | bell    | bell-a288  | cpu    | hpl         | 1578.8      | 2024-09-18 18:03:45 |

* Cluster Health

    Cluster Health routinely checks and records the nodes availablilty the cluster. It then creates a delta takem against the last timestamp (typically 1 hour previous) which it will use to check and report which nodes have been taken offline, which nodes have been brought back online, and the most common reason for nodes being taken offline. Furthermore, an end of day report provides summary data on the current node availability on any given cluster.

    As compared to benchmarking data, which is stored in a Postgres database, Cluster health is simply stored in JSON files with the node name, reason, user, and timestamp:

    ```json
    [{"name": "bell-a146", "reason": "IB? socket not connected", "user": "root", "timestamp": "2024-03-26T13:52:51"}, 
    {"name": "bell-a308", "reason": "no mgmt", "user": "root", "timestamp": "2024-03-27T13:08:56"},
    {"name": "bell-a323", "reason": "no mgmt", "user": "root", "timestamp": "2024-03-27T13:38:47"}, 
    {"name": "bell-a386", "reason": "IB down", "user": "root", "timestamp": "2024-03-27T13:52:07"}]
    ```

## Installation

### Dependenices

#### Python Environment
CANARI requires several python packages for interfacing with databases and generating figures. Using Anaconda package and environment management, a suitable environment can be generated as follows:

```bash
conda create -n canari python=3.12 --yes
conda install psycopg2 sqlite pandas matplotlib seaborn requests -n canari --yes
conda install python-dotenv -c conda-forge -n canari --yes
conda install statsmodels -n canari --yes
conda activate canari
```
#### Cluster dependencies
CANARI is designed to work with clusters using Slrum, and requires many Slurm associated commands to function properly.

At a minimum, the backend nodes must follow a naming scheme in which the the nodes contain the type of the node. We have extensively tested nodes that follow the following naming-schemes:
* clustername-a005
* a005

##### Points of Modification:
Other naming schemes can be accommodated by altering the regex in the following functions in ```canari.py```:

* ```decouple_nodes```
* ```get_nodetype_from_host```

Furthermore, the logic for defining the global ```cluster``` variable in ```canari.py``` and ```report.py``` may need to be altered.

### Configuration
In order to function across multiple clusters, where each may have different naming schemes and directory structures, we push (almost) all variable components into configuration files that can be specified for each cluster. This makes deployment onto a new cluster (in theory) as easy as adding an entry for that configuration file. The specifics of configurations are shown below.


#### Configuration Files
The ```cluster_data.json``` file contains information regarding the nodes, accounts and partitions, and file paths available to each of the clusters. Information pulled from here will be used to configure Slurm submissions on any given cluster.

```json
{
    "cluster_c":{"nodes":{"a":{"devices":["cpu"], "count":100},
                          "b":{"devices":["cpu"], "count":20},
                          "g":{"devices":["cpu", "gpu"], "count":8}
                },
                "primary_node":"a", 
                "database_login_file":"/path/to/db_credentials.json", 
                "benchmarking_apps_path":"/path/to/benchmarking_apps", 
                "slack_token_path": "/path/to/slack/token", 
                "health_data_dir":"/path/to/health/data/for/cluster_c",
                "working_dir":"/path/to/scratch/space",
                "env_dir":"/path/to/anaconda/envs/canari",
                "account":"account-name", 
                "partition":"partition-name",
                "gpu_required":false
                }
}
```

* ```primary_node``` is the default node type that will be used if no node type is provided.
* ```database_login_file``` points to the path of the ```db_credentials.json``` file (see below). 
* ```benchmarking_apps_path``` is the root directory in which the submission files for STREAM and HPL exist, the structure of the subdirectories in this directories is described below.
* ```slack_token_path``` is the path in which the Slack API token is available. 
* ```health_data_dir``` is the directory in which health data for this cluster will be stored. As health data is routinely recycled, these are stored as time-stamped JSON files in this directory as opposed to a relational database like the benchmarking data is. 
* ```working_dir``` is a scratch directory where temporary results and logs will be written.
* ```env_dir``` is the directory of the conda environment that will be passed to backend scripts.
* ```account``` is the Slurm account that backend jobs should be submitted under.
* ```partition``` is the Slurm partition that backend jobs should be submitted under.
* ```gpu_required``` is a flag that will force all jobs to request a GPU in Slurm. Some clusters in our facility have a policy that forces all submitted jobs to request a GPU.

The ```db_credentials.json``` file should be configured with all information necessary to connect with the SQL database. All interactions with the database will first attempt to connect to a Postgres database with the first 5 credentials. If it is unable to connect, the script will fall back to an sqlite3 ```.db``` file.

```json
{"database":{"host":"host.edu", 
            "port":1111,
            "dbname":"postgres",
            "user":"postgres", 
            "password": "password123",
            "sqlitefile":"/path/to/sqlite_backup.db"
            }
}
```
* ```host```, ```port```, ```dbname```, ```user```, and ```password``` are the credentials used to log into the Postgres database. 
* ```sqlitefile``` is a fallback in the event that a Postgres database does not exist, the script will automatically create an sqlite ```.db``` file at this location
    * In either case, a table titled ```benchmark_results_simple``` will be created in the database if it does not exist.

For STREAM and HPL benchmarking, a directory hierarchy within the ```benchmarking_apps_path``` (defined in ```cluster_data.json```) should be made as follows. Each ```submit.sh``` Slurm submission script can run a STREAM or HPL benchmark optimized for each node type available on the cluster.

```
benchmarking_apps_path
├── cluster_c
│   ├── hpl
│   │   └── cpu
│   │       └── submit
│   │           ├── node_type
│   │           │   ├── a
│   │           │   │   └── submit.sh
│   │           │   ├── b
│   │           │   │   └── submit.sh
│   │           │   └── g
│   │           │       └── submit.sh
│   │           └── submit.sh
│   └── stream
│       └── submit
│           └── submit.sh
```

#### Benchmarking Logging file
The ```update_db/update_db.sh``` logging job is dependent on, and ran after the benchmarking job has been completed and logs the benchmark result to the database. This script is passed all necessary parameters by ```canari.py```, but consideration should be made to ensure the environment is being activated correctly. 

## Usage
Once all dependencies have been satisfied, and the configuration files have been filled in, you can collect and report data using the following commands:

### Collecting Benchmarking Data
For regular benchmark logging, we recommend configuring a ```cron``` job on each cluster to submit jobs for some percentage (we use 2.5%) of the cluster every 12 hours.

* To benchmark on a predefined number of nodes, you may run the following command:

    ```python canari.py --num_nodes 20```

    By default, it will run using the ```primary_node``` as defined in the ```cluster_data.json``` file. You can specify which node types you'd like to run on with the ```--node_type``` flag, which supports submitting to multiple node types simultaneously.

* To benchmark on a specific list of hosts, in this case named ```cluster_c-a[001,021,031]```, you can run the following command:

    ```python canari.py --hosts cluster_c-a001, cluster_c-a021, cluster_c-a031```

    If you would like to run on all hosts, you can pass ```--hosts all```, which will submit benchmarking jobs to all online nodes.

    * Since STREAM jobs finish very quickly, and subsequent stream jobs are likely to fall on the same node, you can force STREAM jobs to wait on a dummy "flag" job to spread out the node distribution with the ```--flag_job``` option:

        ```python canari.py --num_nodes 20 --flag_job```

### Collecting Cluster Health Data

By default, only the last 168 health checks are kept. For the purposes of creating a delta, data older than 90 minutes is considered stale, and a report will not be generated until the next run.

* Collecting health data can be done directly with ```canari.py```. This will log data to the ```health_data_dir``` specified in ```cluster_data.json```. 

    ```python canari.py --health_log```

    Although this will log health data, we recommend running the "health summary" in ```report.py``` every hour, which will both log data, and report to Slack.


### Reporting to Slack

Reporting of data is handled by ```report.py```, which reads benchmarking data from the Postgres database, or health data from the health_data_dir. In order to report to Slack, you must have created a Slack app whose access is scoped to write messages and files to your Slack space.  

* To report cluster benchmarking data to Slack, the following command can be used:

    ```python report.py --benchmark --timespan month --slack_channel_id XXXXXXXXXXX```

* To report a current cluster health summary to Slack, the following command can be used:

    ```python report.py --healthsum --slack_channel_id XXXXXXXXXXX```

* To report a healthcheck against previously collected timestamps, the following command can be used:

    ```python report.py --healthcheck --tolerance 0.025 --slack_channel_id XXXXXXXXXXX``` 

    Which will report to slack of there has been a change in availability on more than 2.5% of nodes on the cluster. This will also log health data to ```health_data_dir```. We recommend running this script every hour.

* If a benchmark was ran on an entire cluster (usually during a maintenance), and you'd like to report the perormance foa ll nodes, the following comman may be used:

    ```python report.py --maintenancecheck --log_level=debug --timespan=day --date=2024-10-06 --slack_channel_id XXXXXXXXXXX```

For either ```canari.py``` or ```report.py```, the individual slurm commands being ran can be seen with the ```--log_level=debug``` flag. A full list of options can be seen with ```canari.py -h``` or ```report.py -h``` respectively. 
