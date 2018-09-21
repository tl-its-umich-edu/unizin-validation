# simple.py - very simple example of plain DBAPI-2.0 usage
#
# currently used as test-me-stress-me script for psycopg 2.0
#
# Copyright (C) 2001-2010 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

UNIZIN_FILE = "unizin_{table}.csv"

## don't modify anything below this line (except for experimenting)

import sys
import psycopg2
import os
import itertools

import numpy as np
import pandas as pd

import dbqueries

from collections import OrderedDict
from operator import itemgetter

from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()

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
    df[indexname] = pd.to_numeric(df[indexname], errors='coerce')

    df = df.fillna('NoData')
    return df.set_index(indexname, drop=False)

RESULTS_FILE = open("results.txt", "w")

def compare_CSV(tablename):
    RESULTS_FILE.write(f"Comparing on {tablename}\n")
    sis_file = dbqueries.QUERIES[tablename]['sis_file'].format(date=os.getenv("SIS_DATE"))
    index = dbqueries.QUERIES[tablename]['index']
    try:    
        SIS_df = load_CSV_to_dict(sis_file.format(table=tablename), index)
        Unizin_df = load_CSV_to_dict(UNIZIN_FILE.format(table=tablename), index)
    except Exception as e:
        print ("Exception ",e)
        return

    SIS_len = len(SIS_df)
    Unizin_len = len(Unizin_df)

#    print (SIS_df)
#    print (Unizin_df)
    
    print ("Unizin Len:", Unizin_len)
    print ("SIS Len", SIS_len)

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

        for head in Unizin_head:
            try:
                if SIS_r[head] != Unizin_r[head]:
                    RESULTS_FILE.write(f"{head} does not match for {indexval} SIS: {SIS_r[head]} Unizin: {Unizin_r[head]}\n")
            except:
                continue


def load_Unizin_to_CSV(tablename):
    out_filename = UNIZIN_FILE.format(table=tablename)
    print (f"Loading ucdm {tablename} table to {out_filename}")
    conn = psycopg2.connect(os.getenv("DSN"))
    curs = conn.cursor()

    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER FORCE QUOTE *".format(dbqueries.QUERIES[tablename]['query'])
    UWriter = open(out_filename,"w")
    curs.copy_expert(outputquery, UWriter)

select_tables = ['course_section_enrollment']

print ("""Choose an option.
    1 = Import Unizin Data from GCloud to CSV (need developer VPN or other connection setup)
    2 = Load/Compare all CSV files
    3 = Load/Compare eveything except select table(s))
    4 = Load/Compare only select table(s)s""")
print ("'Select table(s)s' are: ", ', '.join(select_tables))
option = input()

if option == "1":
    for key in dbqueries.QUERIES.keys():
      load_Unizin_to_CSV(key)
if option == "2":
    for key in dbqueries.QUERIES.keys():
        compare_CSV(key)
if option == "3":
    for key in dbqueries.QUERIES.keys():
        if key in select_tables:
            continue
        compare_CSV(key)
if option == "4":
    for key in dbqueries.QUERIES.keys():
        if key not in select_tables:
            continue
        compare_CSV(key)

sys.exit(0)
