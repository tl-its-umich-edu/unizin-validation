# validate.py - Unizin UDW validator
#
# Copyright (C) 2019 University of Michigan Teaching and Learning

UNIZIN_FILE_FORMAT = "unizin_{table}.csv"

## don't modify anything below this line (except for experimenting)

import sys, os, csv, itertools, argparse, smtplib, tempfile, json

from email.mime.text import MIMEText
from collections import OrderedDict 

from dateutil import parser as dparser

import psycopg2

import numpy as np
import pandas as pd

import dbqueries

from collections import OrderedDict
from operator import itemgetter

from tqdm import tqdm


try:
    with open(os.getenv("ENV_FILE", "/unizin-csv-validation/config/env.json")) as f:
        ENV = json.load(f)
except FileNotFoundError as fnfe:
    print("Default config file or one defined in environment variable ENV_FILE not found. This is normal for the build, should define for operation")
    # Set ENV so collectstatic will still run in the build
    ENV = os.environ

OUT_DIR = ENV.get("TMP_DIR", "/tmp/")
RESULTS_FILE = open(OUT_DIR + "u_results.txt", "w")
ERRORS_FILE = open(OUT_DIR + "u_errors.txt", "w")

def load_unizin_to_csv(tablename):
    out_filename = OUT_DIR + UNIZIN_FILE_FORMAT.format(table=tablename)
    print (f"Loading {tablename} table to {out_filename}")
    # The DSN might switch depending on the data file
    conn = psycopg2.connect(ENV.get("DSN_"+dbqueries.QUERIES[tablename]['dsn']))
    
    curs = conn.cursor()

    query = dbqueries.QUERIES[tablename]
    if (query.get('prequery')):
        curs.execute(query.get('prequery'))
    UWriter = open(out_filename,"w")
    # Try this first with this, breaks in 8.0 though :()
    try:
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER FORCE QUOTE *".format(query.get('query'))
        curs.copy_expert(outputquery, UWriter)
    except psycopg2.ProgrammingError:
        print ("Copy query failed, trying regular query, possibly Postgres 8.0")
        writer = csv.writer(UWriter)

        conn.reset()
        curs.execute(query.get('query'))
        writer.writerows(curs.fetchall())

def email_results(filenames, subject=None):
    #Email results, all filenames should be csv with key/value pairs
    msg_text = ""
    # If the file name is not a list make it a list
    od = OrderedDict()
    if not isinstance(filenames, list):
        filenames = [filenames,]
    for filename in filenames:
        rows = csv.reader(open(filename, 'r'))
        for row in rows:
            msg_text += f"{row[0]} : {row[1]}\n"
            od[row[0]] = row[1]
    # Create a text/plain message
    msg = MIMEText(msg_text)

    if (not subject):
        subject = f"CSV Validation Email"
    # Try to append the date to the subject
    canvas_date = dparser.parse(od.get('canvasdatadate'))
    if canvas_date:
        subject = f"{subject} for {canvas_date:%B %d, %Y}"
    msg['Subject'] = subject
    msg['From'] = ENV.get("SMTP_FROM")
    msg['To'] = ENV.get("SMTP_TO")
    msg['Reply-To'] = ENV.get("SMTP_TO")
    msg['Precedence'] = 'list'

    print (f"Emailing out {filename}")
    server = smtplib.SMTP(ENV.get("SMTP_HOST"), ENV.get("SMTP_PORT"), None, 5)
    server.send_message(msg)
    server.quit()

#select_tables = ['academic_term']
select_tables = list(csv.reader([ENV.get("SELECT_TABLES", "academic_term")]))[0]

print (select_tables)

load_unizin_to_csv("number_of_courses_by_term")
load_unizin_to_csv("unizin_metadata")

subject = dbqueries.QUERIES["number_of_courses_by_term"].get('query_name')
email_results([OUT_DIR + UNIZIN_FILE_FORMAT.format(table="unizin_metadata"),OUT_DIR + UNIZIN_FILE_FORMAT.format(table="number_of_courses_by_term")], subject=subject)

RESULTS_FILE.close()
ERRORS_FILE.close()

sys.exit(0)
