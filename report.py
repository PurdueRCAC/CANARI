#io handling
import argparse
import json
import socket
import requests
import os
#Data Handling
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import re
import tempfile
import statistics
from contextlib import contextmanager
#Plotting
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('dark')
#Typing
from typing import Union, Optional
import logging

#Stuff that needs to talk to sentinel
from canari import submit_benchmarking_pair, get_cluster_data
from canari import get_total_nodes
from canari import get_offline_node_objs
from canari import log_status
from canari import get_last_healthcheck
from canari import get_nodetype_from_host


###      Global Variables Begin      ###
health_check_apps = ['hpl', 'stream']  #Applications comprising a health check
hostname=socket.gethostname()
old_style=hostname.split('-')
cluster = old_style[0] if len(old_style) > 1 else hostname.split('.')[1]

#Directory that this script(that you are reading right now) is located in. Use to reference relative files
repo_dir:str = os.path.dirname(__file__)
###       Global Variables End       ###

################################# COMMON FUNCTIONS ###########################################333

def send_slack_message(message, channel_id="C068GC6QWCV", token_path="/home/rderue/.slack/token", blocks=None, debug=True):
    """
    Utilizes the Slack API to send a message to the specified channel through the app whose token
    is contained at token_path. The app must be installed on the workspace you intend to use it on.
    You can use the blocks variable to craft richer messages by doing things like
    using JSON strings to encode Markdown, etc.


    message:    The string you want to send in Slack.
    channel_id: The ID of the channel you want to send the message to. A hack
                for retrieving this value is to open the Slack channel in the
                web-app version of Slack. The channel_id is the string in the
                last /<>/ of the URL.
    token_path: The path to the token for your Slack app. The token must have the
                scope chat:write for this.
    blocks:     A Slack construct that allows rich formatting of messages using
                Markdown.
    """
    if debug:
        channel_id="C068CJDV9V2"
    token=None
    with open(token_path, 'r') as f1:
        token=f1.read()[:-1]
    return requests.post('https://slack.com/api/chat.postMessage', {
        'token': token,
        'channel': channel_id,
        'text': message,
        'username': "Cluster Health Notifications",
        'blocks': json.dumps(blocks) if blocks else None
    }).json()

def upload_slack_file(f, ts, channel_id="C068GC6QWCV", token_path="/home/rderue/.slack/token", blocks=None, debug=False,
        upload_file=None):
    """
    Description: A helper function for uploading a file to slack. Be warned Slack's
    API seems to be able to return 200 OK even when this fails.

    f:          The path to the file you want to upload.
    ts:         The id of the thread to attach this file to. If this is empty it
                will send as its own thread. This value should reflect the id of
                the thread for the oldest message in the thread.
    channel_id: The ID of the channel you want to send the message to. A hack
                for retrieving this value is to open the Slack channel in the
                web-app version of Slack. The channel_id is the string in the
                last /<>/ of the URL.
    token_path: The path to the token for your Slack app. The token must have the
                scope file:write for this 
    blocks:     A Slack construct that allows rich formatting of messages using
                Markdown.
    """
    if debug:
        channel_id="C068CJDV9V2"
    token=None
    with open(token_path, 'r') as f1:
        token=f1.read()[:-1]
    headers = {
        "Authorization": f"Bearer {token}" 
    }
    if upload_file is None:
        upload_file = {
            'file' : (f"{cluster}_health.log", open(f, 'rb'), 'text/plain')
                }
    payload={
        'channels':channel_id,
        'thread_ts':ts,
            }
    response = requests.post('https://slack.com/api/files.upload', headers=headers, params=payload, files=upload_file)
    return response

#################################  CLUSTER HEALTH FUNCTIONS ##########################################
def get_summary(debug=False, token_path="/home/rderue/.slack/token", 
                partition = "testpbs", slack_channel_id = "C068GC6QWCV"):  #Only looks at current timepoint - Does not read/write any data - Put in report.py
    """
    A function which reports the total percent of offlined nodes on the cluster and 
    attaches a chronologically sorted log containing a list of nodes that have been
    down and why they were marked down. This function's intended use case is to be
    ran every weekday for an end of day report.
    """
    total_nodes=get_total_nodes(partition = partition)
    offline_nodes=get_offline_node_objs()
    percent_offline=100*(len(offline_nodes) / float(total_nodes))
    response = send_slack_message("{:.2f}% of {} nodes are offline ({} nodes).".format(percent_offline, cluster.capitalize(), len(offline_nodes)),
                                  debug=debug, token_path = token_path, channel_id = slack_channel_id)
    message=""
    for node in get_offline_node_objs():  #Returns list of "Node" objects
        message += "{} has been offline for {} days for reported reason: {}.".format(node.name, (datetime.now() - node.get_datetime()).days, node.reason) + "\n"
    with tempfile.NamedTemporaryFile(mode="w+") as tmp:
        tmp.write(message)
        tmp.read()
        print(upload_slack_file(tmp.name, response['ts'], debug=debug, 
                                token_path = token_path, channel_id = slack_channel_id))


def healthcheck(critical_percent=.025, debug=False,  token_path="/home/rderue/.slack/token", 
                health_dir = "/depot/itap/verburgt/repos/cluster_health/cluster_health_data/", 
                partition = "testpbs", 
                slack_channel_id = "C068GC6QWCV"):   #Retrieves previous data, logs current data, AND reports  -- Split up?
    """
    A check ran hourly which retrieves the currently offlined nodes, logs that
    information, and reports in Slack if the difference between the currently
    offlined nodes and those logged in the previous healthcheck are more than
    critical_percent different than one another.
    """
    total_nodes=get_total_nodes(partition = partition)
    offline_nodes=get_offline_node_objs()
    last_offline_nodes=get_last_healthcheck(filepath = health_dir)
    # If last healthcheck ran more than 90 minutes ago, then the cronjob
    # probably died. Skip reporting the stale data.
    if last_offline_nodes == None:
        print("Data is stale. Logging current status and exiting...")
        log_status(filepath = health_dir)
        return
    curr_offline=set([str(n) for n in offline_nodes])
    last_offline=set([str(n) for n in last_offline_nodes])
    onlined_nodes = list(last_offline.difference(curr_offline))
    offlined_nodes = list(curr_offline.difference(last_offline))
    reason_offlined_nodes = [x.reason for x in offline_nodes if x.name in offlined_nodes]
    onlined_nodes.sort()
    offlined_nodes.sort()
    if len(onlined_nodes) != 0:
        print(f"Nodes: {onlined_nodes} brought back online since last check.")
    else:
        print("No new nodes brought online since the last check.")
    if len(offlined_nodes) != 0:
        print(f"Nodes: {offlined_nodes} went down since last check.")
    else:
        print("No new nodes went down since the last check.")
    log_status(filepath = health_dir)
    # If the total number of nodes onlined and offlined were more than
    # the "critical_percent" of the cluster, we should report this.
    if float(len(offlined_nodes) + len(onlined_nodes)) / float(total_nodes) > critical_percent:
        if len(offlined_nodes) == 0:
            warning_message="There has been a change in node availabilty on {} affecting more than {:.2f}% of the cluster in the past hour. {} nodes brought back online ({}).".format(cluster, 100*critical_percent, len(onlined_nodes), onlined_nodes)
        elif len(onlined_nodes) == 0:
            warning_message="There has been a change in node availabilty on {} affecting more than {:.2f}% of the cluster in the past hour. {} nodes have gone offline ({}). Most common reason for newly offlined nodes is '{}'.".format(cluster, 100*critical_percent, len(offlined_nodes), offlined_nodes, statistics.mode(reason_offlined_nodes))
        else:
            warning_message="There has been a change in node availabilty on {} affecting more than {:.2f}% of the cluster in the past hour. {} nodes brought back online ({}). {} nodes have gone offline ({}). Most common reason for newly offlined nodes is '{}'.".format(cluster, 100*critical_percent, len(onlined_nodes), onlined_nodes, len(offlined_nodes), offlined_nodes, statistics.mode(reason_offlined_nodes))

        send_slack_message(warning_message, debug=debug, token_path = token_path, channel_id = slack_channel_id)


########################################### CLUSTER BENCHMARK FUNCTIONS  #################################################

@contextmanager
def get_db_connection(**kwargs):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=kwargs.get('dbname'),
            user=kwargs.get('user'),
            password=kwargs.get('password'),
            host=kwargs.get('host'),
            port=kwargs.get('port')
        )
        logging.debug("Connected to Postgres database!")
        yield conn
    except OperationalError as e:
        logging.warning("Unable to connect to Postgres database - Falling back to SQLite3")
        conn = sqlite3.connect(kwargs.get('sqlitefile')) #type: ignore
        yield conn
    except Exception as e:
        raise ConnectionError("Unable to connect to database")

    finally:
        if conn is not None:
            conn.close()

def query_database(db_params:dict, query:str) -> pd.DataFrame:

    with get_db_connection(**db_params) as conn:
        logging.debug(f"query is '{query}'")  #query.as_string(conn)
        df = pd.read_sql_query(query, conn)

        df["nodetype"] = df.node.map(get_nodetype_from_host)
        df["outlier"] =  df.performance < (df.performance.mean() - df.performance.std() * 3)

        logging.debug(f"fetched {df.shape[0]} rows!")
        return df

def get_query(cluster:str, application:str, 
              timespan:Optional[str] = "week", 
              table:str = "benchmark_results_simple") -> str:

    assert timespan in ['week', 'month', 'year']
    time_deltas = {'week': 7,'month': 30,'year': 365}
    # Calculate end and begin datetimes
    end_datetime = datetime.now() + timedelta(days=1)
    begin_datetime = end_datetime - timedelta(days=time_deltas[timespan])
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    begin_date_str = begin_datetime.strftime('%Y-%m-%d')
    
    query = f"""
            SELECT * FROM {table}
            WHERE cluster = '{cluster}' 
            AND application = '{application}' 
            AND datetime BETWEEN '{begin_date_str}' AND '{end_date_str}';
        """    
    return query


def generate_stream_graph(stream_df: pd.DataFrame, bynodetype:Optional[bool] = True) -> str:
    stream_df['datetime'] = pd.to_datetime(stream_df['datetime'])
    stream_df['datetime'] = stream_df['datetime'].dt.floor('12H') #Floor to the nearest half day

    #Set up Figure
    fig, ax  = plt.subplots(1, 1, figsize=(15,8))
    fig.set_tight_layout(True) #type:ignore
    ax.set_title('Average STREAM Performance Over Time')
    ax.set_ylabel('STREAM Performance (MB/s)')
    ax.set_xlabel('Time of Run')

    #Plot overall data
    sns.lineplot(data=stream_df, x='datetime', y='performance', ax=ax, label = "overall")#, marker="o")
    #sns.scatterplot(data= stream_df, x='datetime', y='performance', ax=ax, color="black", marker=".")
    #Plot by node type
    if bynodetype:
        node_groups = stream_df.groupby("nodetype")
        for nodetype, node_df in node_groups:
                sns.lineplot(data=node_df, x='datetime', y='performance', ax=ax, label = nodetype)
    
    #sns.lineplot overrides this, keep after plotting
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)

    #Annotate outliers on graph
    outlier_df = stream_df.loc[stream_df.outlier]
    ax.scatter(x=outlier_df['datetime'], y=outlier_df['performance'], color='r')
    for _, row in outlier_df.iterrows():
        ax.annotate(row['node'], (row['datetime'], row['performance']))

    filename='/tmp/stream.png'
    #filename='stream.png'
    fig.savefig(filename)
    return filename


def generate_hpl_graph(hpl_df: pd.DataFrame, bynodetype:Optional[bool] = True) -> str:
    hpl_df['datetime'] = pd.to_datetime(hpl_df['datetime'])
    hpl_df['datetime'] = hpl_df['datetime'].dt.floor('12H') #Floor to the nearest half day

    # hpl_df = hpl_df.loc[hpl_df.datetime < '2024-07-25']
    #Set up Figure
    fig, ax  = plt.subplots(1, 1, figsize=(15,8))
    fig.set_tight_layout(True) #type:ignore
    ax.set_title('Average HPL Performance Over Time')
    ax.set_ylabel('HPL Performance (GFLOPs)')
    ax.set_xlabel('Time of Run')

    #Plot overall data
    sns.lineplot(data=hpl_df, x='datetime', y='performance', ax=ax, label = "overall")#, marker="o")
    # sns.scatterplot(data=hpl_df, x='datetime', y='performance', ax=ax, color="black", marker=".")
    #Plot by node type
    if bynodetype:
        node_groups = hpl_df.groupby("nodetype")
        for nodetype, node_df in node_groups:
                sns.lineplot(data=node_df, x='datetime', y='performance', ax=ax, label = nodetype)
    
    #sns.lineplot overrides this, keep after plotting
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)

    #Annotate outliers on graph
    outlier_df = hpl_df.loc[hpl_df.outlier]
    ax.scatter(x=outlier_df['datetime'], y=outlier_df['performance'], color='r')
    for _, row in outlier_df.iterrows():
        ax.annotate(row['node'], (row['datetime'], row['performance']))

    filename='/tmp/hpl.png'
    #filename='hpl.png'
    fig.savefig(filename)
    # exit()
    return filename


def report_performance(timespan:str, database_login_file:str, debug:bool=True, 
                       token_path:str = f"{repo_dir}/tokens/.slack/token", cluster:str = cluster, 
                       resubmit:bool = True, 
                       slack_channel_id = "C068GC6QWCV") -> None:

    with open(database_login_file, 'r') as file:
        db_params = json.load(file)["database"]

    response = send_slack_message(f"{cluster.capitalize()} performance report for the last {timespan}.",
                                  debug=debug, token_path = token_path, channel_id = slack_channel_id)
    assert response["ok"]
    ts = response["ts"]
    logging.debug(f"ts is {ts}")

    graph_function_map = {
        "stream": generate_stream_graph,
        "hpl": generate_hpl_graph,
    }

    for application in health_check_apps:
        #Get the data
        query = get_query(cluster = cluster, application=application, timespan = timespan)
        application_df = query_database(db_params, query)
        #Create the image
        application_pngpath = graph_function_map[application](application_df) #Create different image based on application
        graph_file = {'file' : (f"{application}.png", open(application_pngpath, 'rb'), 'image/png')}
        #Upload to slack
        upload_response = upload_slack_file(application_pngpath,
                                            channel_id = slack_channel_id,
                                            upload_file = graph_file, 
                                            ts = ts, 
                                            token_path = token_path, 
                                            debug = debug)
        #Resubmit poor performing nodes
        if resubmit:
            outlier_nodes = set(application_df.loc[application_df.outlier, "node"])
            for host in outlier_nodes:
                resubmit_node(application = application, host = host, database_login_file = database_login_file)

def resubmit_node(application, database_login_file, host):
    print(application, host)
    cluster_data = get_cluster_data(cluster=cluster)
    account = cluster_data["account"]
    partition = cluster_data["partition"]
    logging.info(f"Resubmitting {application} on node {host}")
    submit_benchmarking_pair(application, database_login_file = database_login_file, host = host, account = account, partition=partition)
    

############################################# ARGUMENT PARSING ################################################

def main():
    parser = argparse.ArgumentParser()
    #Arguments for configurations
    parser.add_argument("--slack_channel_id", type = str, default= "C068GC6QWCV", help = "Slack channedl ID to send messages to")
    parser.add_argument('--debug', action="store_true", help='Will report to the "cluster_health_debug" channel if provided')
    parser.add_argument('--log_level', type=str, 
                    choices=['debug', 'info', "warning", "error", "critical"], default = "warning", help='Set the logging level')
    
    #Arguments for cluster_benchmarking reporting
    parser.add_argument('--benchmark', action="store_true", help='Will resubmit nodes with outlier performance if provided')
    parser.add_argument('--timespan', type=str, 
                    choices=["week", "month", "year"], default = "week", help='Timespan to report')
    parser.add_argument('--resubmit', action="store_true", help='Will resubmit nodes with outlier performance if provided')


    #Arguments for cluster_health reporting
    parser.add_argument('--healthcheck', action="store_true", help='Will run a healthcheck and log to Slack')
    parser.add_argument('--healthsum', action="store_true", help='WWill print the EOD Health summary to Slack')
    parser.add_argument('-t', '--tolerance', metavar='%', type=float, default=0.025,
        help='A float representing the percent (as a decimal) change in node availability we can silently ignore.')
    
    parser.add_argument('--health_dir',type=str, default="/depot/itap/verburgt/repos/cluster_benchmarking/cluster_health_data/",
                         help='Default path for storing health_data JSON files')
    
    cluster_data = get_cluster_data(cluster=cluster)
    token_path = cluster_data["slack_token_path"]
    database_login_file = cluster_data["database_login_file"]
    health_data_dir = cluster_data["health_data_dir"]
    partition = cluster_data["partition"]

    #Process arguments
    args = parser.parse_args()
    #Set up logging
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

    #Run
    if not any([args.healthcheck, args.healthsum, args.benchmark]):
        raise ValueError("one of healthcheck, healthsum, or benchmark must be set!")
    
    if args.benchmark:
        report_performance(timespan=args.timespan, 
                           cluster = cluster, 
                           token_path=token_path,
                           debug = args.debug, 
                           database_login_file = database_login_file, 
                           resubmit=args.resubmit,
                           slack_channel_id = args.slack_channel_id)
    
    if args.healthcheck:
        healthcheck(critical_percent=args.tolerance, 
                    debug=args.debug, 
                    health_dir = health_data_dir,
                    token_path = token_path,
                    partition = partition, 
                    slack_channel_id = args.slack_channel_id)

    if args.healthsum:
        get_summary(debug=args.debug, 
                    token_path = token_path, 
                    partition = partition, 
                    slack_channel_id = args.slack_channel_id)



if __name__ == "__main__":
    main()