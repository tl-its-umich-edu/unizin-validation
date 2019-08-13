# validate.py - Unizin UCDM and UCW validator
#
# Copyright (C) 2018 University of Michigan Teaching and Learning

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

class SimpleQuoter(object):
    @staticmethod
    def sqlquote(x=None):
        return "'bar'"

def lower_first(iterator):
    return itertools.chain([next(iterator).lower()], iterator)

def load_CSV_to_dict(infile, indexname):
    df = pd.read_csv(infile, delimiter=',', dtype=str)
    # Lower case headers
    df.columns = map(str.lower, df.columns)
    df[indexname] = pd.to_numeric(df[indexname], errors='coerce', downcast='integer')

    df = df.fillna('NoData')
    return df.set_index(indexname, drop=False)

#def close_compare(a, b):
#    if (isinstance(a, float) and isinstance(b,float)):
#        return np.isclose(a,b)
#    return a == b

def close_compare(i, j):
    try:
        i = float(i)
        j = float(j)
        return np.isclose(i,j)
    except ValueError:
        return i==j

def compare_CSV(tablename):
    RESULTS_FILE.write(f"Comparing on {tablename}\n")
    sis_file = dbqueries.QUERIES[tablename]['sis_file'].format(date=ENV.get("SIS_DATE"))
    index = dbqueries.QUERIES[tablename]['index']
    try:    
        SIS_df = load_CSV_to_dict(sis_file.format(table=tablename), index)
        Unizin_df = load_CSV_to_dict(OUT_DIR + UNIZIN_FILE_FORMAT.format(table=tablename), index)
    except Exception as e:
        print ("Exception ",e)
        return

    SIS_len = len(SIS_df)
    Unizin_len = len(Unizin_df)

#    print (SIS_df)
#    print (Unizin_df)
    
    RESULTS_FILE.write ("Unizin rows: %d, SIS rows: %d\n" % (Unizin_len, SIS_len))

    if len(Unizin_df) == 0 or len(SIS_df) == 0:
        RESULTS_FILE.write(f"This table {tablename} has a empty record for at least one dataset, skipping\n")
        return

    lendiff = Unizin_len - SIS_len
    if lendiff > 0:
        RESULTS_FILE.write("Unizin has %d more rows than SIS for this table\n" % abs(lendiff))
    elif lendiff < 0:
        RESULTS_FILE.write("SIS has %d more rows than Unizin for this table\n" % abs(lendiff))

    Unizin_head = list(Unizin_df)
    print (Unizin_head)

    RESULTS_FILE.flush()
    for i, SIS_r in tqdm(SIS_df.iterrows(), total=SIS_len):
        #Look at all the unizin headers and compare
        indexval = SIS_r[index]
        if not indexval:
            continue
        try: 
            Unizin_r = Unizin_df.loc[indexval]
        except:
            #print (f"Index error on {indexval}")
            continue

#        f = np.frompyfunc(close_compare, 2, 1)

        for head in Unizin_head:
            try:
#               f(SIS_r[head], Unizin_r[head])
                if not close_compare(SIS_r[head], Unizin_r[head]):
#                    RESULTS_FILE.write("type SIS %s, type Unizin %s\n" % (type(SIS_r[head]),type(Unizin_r[head])))
                    RESULTS_FILE.write(f"{head} does not match for {indexval} SIS: {SIS_r[head]} Unizin: {Unizin_r[head]}\n")
            except Exception as e:
                ERRORS_FILE.write("%s\n" % str(e)) 
                continue

def load_Unizin_to_CSV(tablename):
    out_filename = OUT_DIR + UNIZIN_FILE_FORMAT.format(table=tablename)
    print (f"Loading ucdm {tablename} table to {out_filename}")
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
    msg['Precedence'] = 'bulk'

    print (f"Emailing out {filename}")
    server = smtplib.SMTP(ENV.get("SMTP_HOST"), ENV.get("SMTP_PORT"), None, 5)
    server.send_message(msg)
    server.quit()

#select_tables = ['academic_term']
select_tables = list(csv.reader([ENV.get("SELECT_TABLES", "academic_term")]))[0]

print (select_tables)

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--option", type=int, help="Run an option by number",)
args = parser.parse_args()

option = args.option

if (not option):
    print ("""Choose an option.
    1 = Import *All* Unizin Data from GCloud to CSV (need developer VPN or other connection setup)
    2 = Import *select* table(s) from GCloud to CSV (need developer VPN or other connection setup)
    3 = Load/Compare *All* CSV files
    4 = Load/Compare *select* table(s)
    5 = Email/Verification job data loaded into Unizin (Special Case)
    """)
    print ("'Select table(s)' are: ", ', '.join(select_tables))
    option = int(input())

print (f"Running option {option}")
if option == 1:
    for key in dbqueries.QUERIES.keys():
        load_Unizin_to_CSV(key)
elif option == 2:
    for key in dbqueries.QUERIES.keys():
        if key in select_tables:
            load_Unizin_to_CSV(key)
elif option == 3:
    for key in dbqueries.QUERIES.keys():
        compare_CSV(key)
elif option == 4:
    for key in dbqueries.QUERIES.keys():
        if key in select_tables:
            compare_CSV(key)
elif option == 5:
    # Only run these queries
    load_Unizin_to_CSV("number_of_courses_by_term")
    load_Unizin_to_CSV("unizin_metadata")
    subject = dbqueries.QUERIES["number_of_courses_by_term"].get('query_name')
    email_results([OUT_DIR + UNIZIN_FILE_FORMAT.format(table="unizin_metadata"),OUT_DIR + UNIZIN_FILE_FORMAT.format(table="number_of_courses_by_term")], subject=subject)
else: 
    print(f"{option} is not currently a valid option")

RESULTS_FILE.close()
ERRORS_FILE.close()

sys.exit(0)
