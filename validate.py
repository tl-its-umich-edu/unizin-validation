# validate.py - Unizin Data Warehouse validator

# Copyright (C) 2019 University of Michigan ITS Teaching and Learning

# Local modules
from dbqueries import QUERIES
from jobs import JOBS

# Standard modules
import json, logging, os, sys
from datetime import datetime
from collections import namedtuple

# Third-party modules
import numpy as np
import pandas as pd
import psycopg2

# Global variables
logger = logging.getLogger(__name__)
logging.basicConfig()
FLAG = " <-- "

try:
    with open(os.getenv("ENV_FILE", "config/env.json")) as f:
        ENV = json.load(f)
except FileNotFoundError as fnfe:
    logger.info("Default config file or one defined in environment variable ENV_FILE not found. This is normal for the build, should define for operation.")
    # Set ENV so collectstatic will still run in the build
    ENV = os.environ

OUT_DIR = ENV.get("OUT_DIR", "data/")
logger.setLevel(ENV.get("LOG_LEVEL", "DEBUG"))


# Classes

class DBConnManager:

    def __init__(self):
        self.db_conns = {}

    def establish_db_connection(self, db_name):
        db_config = ENV["DATA_SOURCES"][db_name]
        if db_config['type'] == "PostgreSQL":
            conn = psycopg2.connect(**db_config['params'])
            self.db_conns[db_name] = conn
        else:
            logger.debug(f"The database type {db_config['type']} was not recognized")
            # If we need other database connections, we can add these here.
            # A catch-all can could be creating an engine with SQLAlchemy.

    def set_up(self, query_key_strings):
        data_sources_for_queries = [QUERIES[query_key_string]['data_source'] for query_key_string in query_key_strings]
        data_sources_used_by_job = pd.Series(data_sources_for_queries).drop_duplicates().to_list()
        for data_source in data_sources_used_by_job:
            logger.info(f"Connecting to {data_source}")
            self.establish_db_connection(data_source)

    def tear_down(self):
        for db_conn in self.db_conns.values():
            db_conn.close()


# Functions

def calculate_table_counts_for_db(table_names, db_conn_obj):
    table_count_dfs = []
    for table_name in table_names:
        count = pd.read_sql(f"""
            SELECT COUNT(*) AS record_count FROM {table_name};
        """, db_conn_obj)
        count_df = count.assign(**{"table_name": table_name})
        table_count_dfs.append(count_df)
    table_counts_df = pd.concat(table_count_dfs)
    table_counts_df = table_counts_df[['table_name', 'record_count']]
    return table_counts_df


def execute_query_and_write_to_csv(query_dict, db_conn_manager):
    # All output_dfs should be key-value pairs (two columns)
    out_file_path = OUT_DIR + query_dict["output_file_name"]
    db_conn_obj = db_conn_manager.db_conns[query_dict['data_source']]
    if query_dict["type"] == "standard":
        output_df = pd.read_sql(query_dict["query"], db_conn_obj)
    elif query_dict['type'] == "table_counts":
        output_df = calculate_table_counts_for_db(query_dict["tables"], db_conn_obj)
    else:
        logger.debug(f"{query_dict['type']} is not currently a valid query type option.")
        output_df = pd.DataFrame()
    logger.info(f"Writing results of {query_dict['query_name']} query to {out_file_path}")
    with open(out_file_path, "w", encoding="utf-8") as output_file:
        output_file.write(output_df.to_csv(index=False))
    return output_df


def run_checks_on_output(checks_dict, output_df):
    flag_strings = []
    for check_name in checks_dict.keys():
        check = checks_dict[check_name]
        check_func = check['condition']
        output_df[check_name] = output_df.iloc[:, 1]
        if len(check['rows_to_ignore']) > 0:
            first_column = output_df.columns[0]
            row_indexes_to_ignore = output_df[output_df[first_column].isin(check['rows_to_ignore'])].index.to_list()
            output_df[check_name][row_indexes_to_ignore] = np.nan
        output_df[check_name] = output_df[check_name].map(check_func, na_action='ignore')
        if False in output_df[check_name].to_list():
            logger.info(f"Raising {check['color']} flag")
            flag_strings.append(check['color'])
    checked_output_df = output_df
    ChecksResult = namedtuple("ChecksResult", ["checked_output_df", "flags"])
    return ChecksResult(checked_output_df, flag_strings)


def generate_result_text(query_name, checked_query_output_df):
    columns = checked_query_output_df.columns
    result_text = f"{columns[0]} : {columns[1]}\n"
    total_flags = 0
    for row_tup in checked_query_output_df.iterrows():
        row = row_tup[1]
        result_text += f"{row[0]} : {row[1]}"
        # Generate flag labels for check failures
        row_flag_labels = []
        for index, value in row[2:].items():
            if value is False:
                row_flag_labels.append(f'"{index}" condition failed')
        if len(row_flag_labels) > 0:
            flag_text = "; ".join(row_flag_labels)
            result_text += FLAG + flag_text
        result_text += "\n"
        total_flags += len(row_flag_labels)
    result_header = f"\n- - -\n\n** {query_name} **\n"
    if total_flags > 0:
        result_header += f"!! Flagged {total_flags} possible issue(s) !!\n"
    return result_header + result_text


# Main Program

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in JOBS:
        job_key = sys.argv[1]
    else:
        # The default job is the UDW Daily Status Report.
        job_key = "UDW"

    job = JOBS[job_key]
    job_name = job["full_name"]
    query_keys = job["queries"]

    # Set up connections to data sources used by job's queries
    manager = DBConnManager()
    manager.set_up(query_keys)

    # Execute queries and generate results text
    results_text = ""
    flags = []
    for query_key in query_keys:
        query = QUERIES[query_key]
        query_output_df = execute_query_and_write_to_csv(query, manager)
        checks_result = run_checks_on_output(query['checks'], query_output_df)
        flags += checks_result.flags
        results_text += generate_result_text(query['query_name'], checks_result.checked_output_df)

    flags = pd.Series(flags).drop_duplicates().to_list()
    if len(flags) == 0:
        flags.append("GREEN")
    flag_prefix = f"[{', '.join(flags)}]"
    now = datetime.now()
    print(f"{flag_prefix} {job_name} for {now:%B %d, %Y}\n{results_text}")

    # Close connections to data sources
    manager.tear_down()

    if "RED" in flags:
        logger.error("Status is RED")