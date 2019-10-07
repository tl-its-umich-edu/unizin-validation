# validate.py - Unizin Data Warehouse validator
#
# Copyright (C) 2019 University of Michigan ITS Teaching and Learning

# Local modules
from dbqueries import QUERIES

# Standard modules
import json, logging, os, smtplib, sys
from datetime import datetime

# Third-party modules
from email.mime.text import MIMEText
import pandas as pd
import psycopg2, pytz

# Global variables

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)

try:
    with open(os.getenv("ENV_FILE", "config/env.json")) as f:
        ENV = json.load(f)
except FileNotFoundError as fnfe:
    logger.info("Default config file or one defined in environment variable ENV_FILE not found. This is normal for the build, should define for operation.")
    # Set ENV so collectstatic will still run in the build
    ENV = os.environ

OUT_DIR = ENV.get("OUT_DIR", "data/")


# Functions

def establish_db_connection(db_name):
    db_config = ENV[db_name]
    if db_config['type'] == "PostgreSQL":
        conn = psycopg2.connect(**db_config['params'])
    else:
        logger.debug(f"The database type {db_config['type']} was not recognized")
        # If we need other database connections, we can add these here.
        # A catch-all can could be creating an engine with SQLAlchemy.
    return conn


def calculate_table_counts_for_db(db_name, table_names):
    db_conn = establish_db_connection(db_name)
    table_count_dfs = []
    for table_name in table_names:
        count = pd.read_sql(f"""
            SELECT COUNT(*) AS record_count FROM {table_name};
        """, db_conn)
        count_df = count.assign(**{"table_name": table_name})
        table_count_dfs.append(count_df)
    table_counts_df = pd.concat(table_count_dfs)
    table_counts_df = table_counts_df[['table_name', 'record_count']]
    db_conn.close()
    return table_counts_df


def execute_query_and_write_to_csv(query_dict):
    out_file_path = OUT_DIR + query_dict["output_file_name"]
    if query_dict["type"] == "standard":
        db_conn = establish_db_connection(query_dict["data_source"])
        output_df = pd.read_sql(query_dict["query"], db_conn)
        db_conn.close()
    elif query_dict['type'] == "table_counts":
        output_df = calculate_table_counts_for_db(query_dict["data_source"], query_dict["tables"])
    else:
        logger.debug(f"{query_dict['type']} is not currently a valid query type option.")
        output_df = pd.DataFrame()
    logger.info(f"Writing results of {query_key} query to {out_file_path}")
    with open(out_file_path, "w", encoding="utf-8") as output_file:
        output_file.write(output_df.to_csv(index=False))
    return out_file_path


def email_results(results_dict, subject=None):
    # All file_paths should point to a CSV with key-value pairs
    msg_text = ""
    for query_name in results_dict.keys():
        msg_text += '\n- - -\n\n'
        msg_text += f"** {query_name} **\n"
        result_df = pd.read_csv(results_dict[query_name])
        columns = result_df.columns
        msg_text += f"{columns[0]} : {columns[1]}\n"
        for row_tup in result_df.iterrows():
            row = row_tup[1].to_list()
            msg_text += f"{row[0]} : {row[1]}\n"

    # Create a plain text message
    msg = MIMEText(msg_text)
    now = datetime.now(tz=pytz.UTC)
    msg['Subject'] = subject + f" for {now:%B %d, %Y}"
    msg['From'] = ENV.get("SMTP_FROM")
    msg['To'] = ENV.get("SMTP_TO")
    msg['Reply-To'] = ENV.get("SMTP_TO")
    msg['Precedence'] = 'list'

    logger.info(f"Emailing out results to {msg['To']}")
    server = smtplib.SMTP(ENV.get("SMTP_HOST"), ENV.get("SMTP_PORT"), None, 5)
    server.send_message(msg)
    server.quit()


# Main Program

if __name__ == "__main__":
    # Run standard UDW validation process
    subject = "UDW Daily Status Report"
    query_keys = ["unizin_metadata", "number_of_courses_by_term", "udw_table_counts"]
    results = {}
    for query_key in query_keys:
        query = QUERIES[query_key]
        csv_path = execute_query_and_write_to_csv(query)
        results[query["query_name"]] = csv_path
    email_results(results, subject)
    sys.exit(0)
