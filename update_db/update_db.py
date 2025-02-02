import psycopg2
from psycopg2 import sql
import subprocess
from  datetime import datetime as dt
import argparse
import json
import shlex
from typing import Union, Optional
import logging
import os
import re
from contextlib import contextmanager
from psycopg2 import OperationalError
import sqlite3

def get_scontrol_status(job_id:Union[int,str]) -> dict:
    '''Returns the output of scontrol for a particular job ID as a dictionary'''
    command = f"scontrol show job {job_id}"
    job_data = subprocess.run(shlex.split(command), capture_output = True, text = True)
    if job_data.returncode == 0:
        job_data_dict = {row.split("=")[0]:row.split("=")[1] for row in job_data.stdout.strip().split()}
        return job_data_dict
    else:
        print(job_data.stderr)
        raise RuntimeError(f"Invlaid Scontrol command '{command}'")

def get_sacct_status(job_id: Union[int, str], format_keys: Optional[list] = None) -> dict:
    '''Returns the output of sacct for a particular job ID as a dictionary'''
    if format_keys is None:
        format_keys = ["JobID","JobName","Partition","Account","AllocCPUS","State","ExitCode","Start","End","NodeList"]

    command  = f"sacct -j {job_id} --format={','.join(format_keys)} --noheader"
    job_data = subprocess.run(shlex.split(command), capture_output = True, text = True)
    job_result_vals = job_data.stdout.strip().split("\n")[0].split()
    job_result_dict = {key: val for (key, val) in zip(format_keys, job_result_vals)}
    return(job_result_dict)

def gflops_from_hpl(hpl_file:str, permissive:bool = True) -> Union[float, None]:
    '''Reads an HPL output file and extracts the gflops'''
    gflops = None
    with open(hpl_file, 'rt') as f:
        lines = f.readlines()
        for line_num, line in enumerate(lines):
            # Detecting result line
            if re.search(r'T/V\s+N\s+NB\s+P\s+Q\s+Time\s+Gflops', line):
                gflops = float(lines[line_num+2].split()[6])
    if (gflops is not None) or permissive:
        return gflops
    else:
        raise ValueError(f"Cannot find gflops in file '{hpl_file}'")

def triadrate_from_stream(stream_file:str, permissive:bool = True) -> Union[float, None]:
    '''Read Best Triad rate (MB/s) from stream output. '''
    triad_rate= None
    try:
        with open(stream_file, 'rt') as f:
            lines = f.readlines()
            number_of_lines = len(lines)
            triad_rate = float(lines[number_of_lines - 4].split()[1] )
    except (IndexError, ValueError) as e:
        if permissive:
            return triad_rate
        else:
            raise e
        
    return triad_rate


def get_performance(application:str, filepath:str) -> Union[float, None]:
    match application:
        case "hpl":
            return gflops_from_hpl(filepath)
        case "stream":
            return triadrate_from_stream(filepath)
        case _:
            raise NotImplementedError("application must be 'hpl' or 'stream'")


def table_exists(conn, table_name):
    if isinstance(conn, sqlite3.Connection):  #Run this block if SQLITE3
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cursor.fetchone() is not None
    elif isinstance(conn, psycopg2.extensions.connection): #Run this block if POSTGRES
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (table_name,))
        return cursor.fetchone()[0]  #type: ignore
    else:
        raise NotImplementedError("Unsupported database connection type")
    
def create_benchmark_table(conn):
    if isinstance(conn, sqlite3.Connection):  #Run this block if SQLITE3
        cursor = conn.cursor()
        create_table_sql = f"""
    CREATE TABLE benchmark_results_simple (
       result_id INTEGER PRIMARY KEY AUTOINCREMENT,
   	cluster VARCHAR NOT NULL,
   	node VARCHAR NOT NULL,
   	device VARCHAR ,
       application VARCHAR NOT NULL,
       performance NUMERIC NOT NULL,
       datetime TIMESTAMP
   );
   """
        cursor.execute(create_table_sql)

    elif isinstance(conn, psycopg2.extensions.connection): #Run this block if POSTGRES
        cursor = conn.cursor()
        create_table_sql = f"""
       CREATE TABLE benchmark_results_simple (
       result_id SERIAL PRIMARY KEY,
   	   cluster VARCHAR NOT NULL,
   	   node VARCHAR NOT NULL,
   	   device VARCHAR ,
       application VARCHAR NOT NULL,
       performance NUMERIC NOT NULL,
       datetime TIMESTAMP
   );
   """
        cursor.execute(create_table_sql)
    else:
        raise NotImplementedError("Unsupported database connection type")  

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


@contextmanager
def get_cursor(conn):
    """
    Context manager for obtaining a cursor from either SQLite or PostgreSQL.
    Ensures proper cursor management for both databases.
    """
    cursor = None
    try:
        # Create a cursor from the connection object (works for both SQLite and PostgreSQL)
        cursor = conn.cursor()
        yield cursor
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        # Close the cursor explicitly
        if cursor is not None:
            cursor.close()

def insert_row(db_params:dict, cluster:str, 
               node:str, device:str, application:str, 
               performance:float, datetime:dt) -> None:
    '''Inserts a row into the database'''
    try:
        with get_db_connection(**db_params) as conn:

            if not table_exists(conn, "benchmark_results_simple"):
                create_benchmark_table(conn)

            with get_cursor(conn) as cur:
                if isinstance(conn, sqlite3.Connection):  #Run this block if SQLITE3
                    insert_query = """ INSERT INTO benchmark_results_simple 
                                    (cluster, node, device, application, performance, datetime) 
                                    VALUES (?, ?, ?, ?, ?, ?) """

                elif isinstance(conn, psycopg2.extensions.connection):
                    insert_query = """ INSERT INTO benchmark_results_simple 
                                    (cluster, node, device, application, performance, datetime) 
                                    VALUES (%s, %s, %s, %s, %s, %s) """
                else:
                    raise NotImplementedError("Unsupported database connection type")    
                
                cur.execute(insert_query, (cluster, node, device,
                                           application, performance,
                                           datetime))
                conn.commit() 
                
    except (Exception, psycopg2.DatabaseError) as e:
        raise(e)


def row_exists(db_params:dict, cluster:str, 
               node:str, device:str, application:str, 
               performance:float, datetime:dt):
    query = sql.SQL("""
        SELECT 1 FROM benchmark_results_simple
        WHERE cluster = %s AND node = %s AND device = %s AND application = %s AND performance = %s AND datetime = %s
        LIMIT 1
    """)

    try:
        with get_db_connection(**db_params) as conn:
            if not table_exists(conn, "benchmark_results_simple"):
                create_benchmark_table(conn)

            with get_cursor(conn) as cur:
                if isinstance(conn, sqlite3.Connection):
                    query = """
                        SELECT 1 FROM benchmark_results_simple
                        WHERE cluster = ? AND node = ? AND device = ? AND application = ? AND performance = ? AND datetime = ?
                        LIMIT 1;
                        """
                elif isinstance(conn, psycopg2.extensions.connection):
                    query = """
                    SELECT 1 FROM benchmark_results_simple
                    WHERE cluster = %s AND node = %s AND device = %s AND application = %s AND performance = %s AND datetime = %s
                    LIMIT 1;
                    """
                else:
                    raise NotImplementedError("Unsupported database connection type")    
                
                cur.execute(query, (cluster, node, device, application, performance, datetime))
                exists = cur.fetchone() is not None
                return exists
    except (Exception, psycopg2.DatabaseError) as e:
        raise(e)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-c" ,"--cluster", type = str, default = None, help="Cluster test ran on")
    parser.add_argument("-a" ,"--application", type = str, choices=["stream", "hpl"], help="Benchmarking application")
    parser.add_argument("-d" ,"--datetime", type = str, help="Datetime string in %Y-%m-%d-%H%M%S format")
    parser.add_argument("-o" ,"--outfile", type = str, help="Output file from the test", required=True)
    parser.add_argument("-dev" ,"--device", type = str, choices = ["cpu", "gpu"], default = "cpu", help="device test was ran on ")
    parser.add_argument("--delete", action = "store_true", help = "Delete the benchmarking log file upon success")

    parser.add_argument("-l" ,"--database_login_file", type = str, 
                        default = "/depot/itap/verburgt/repos/cluster_benchmarking/db_credentials.json", 
                        help="JSON file containing database login credentials")
    
    parser.add_argument('--log_level', type=str, 
                        choices=['debug', 'info', "warning", "error", "critical"], 
                        default = "warning", help='Set the logging level')
    
    node_source = parser.add_mutually_exclusive_group(required = True)
    node_source.add_argument("-j" ,"--jobid", type = int, help="Job ID")
    node_source.add_argument("-n" ,"--node", type = str, help="node test ran on")

    args = parser.parse_args()
    
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
    

    with open(args.database_login_file, 'r') as file:
        db_params = json.load(file)["database"]


    #Node can either be supplied directly, or be derived from the Job ID
    if args.jobid:
        node = get_sacct_status(args.jobid)["NodeList"]
    elif args.node:
        node = args.node

    cluster = args.cluster
    application = args.application
    performance = get_performance(args.application, args.outfile)
    if performance is None:
        raise RuntimeError("Performance could not be obtained!")
    
    device = args.device
    if args.datetime: #Use the provided datetime
        logging.debug(f"Using provided datetime of {args.datetime}")
        datetime = dt.strptime(args.datetime, "%Y-%m-%d-%H%M%S")
    elif args.jobid: #Use the datetime of the job ID
        datetime = dt.strptime(get_sacct_status(args.jobid)["Start"], "%Y-%m-%dT%H:%M:%S")
        logging.debug(f"Using provided datetime of Job ID {args.jobid}")
    else: #Fall back to the current time
        datetime = dt.now()
        logging.warning(f"No Datetime available! Using Current time")
    logging.debug(f"datetime set to {datetime}")

    
    #Add this benchmarking data to the database if it doesn't already exist
    already_in_db = row_exists(db_params, cluster, node, device, application, performance, datetime)
    if not already_in_db:
        insert_row(db_params, cluster, node, device, application, performance, datetime)
    else:
        print("Already in Database") #TODO add logging instead of printing

    if args.delete:
        logging.info(f"Deleting benchmarking file {args.outfile}!")
        os.remove(args.outfile)


if __name__ == "__main__":
    main()

