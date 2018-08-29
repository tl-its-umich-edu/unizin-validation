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

## put in DSN your DSN string (This should come from the .env file)
DSN = 'dbname=test'

SIS_FILE = '2018-08-27%2FPerson_2018-08-27.csv'
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

from dotenv import load_dotenv
load_dotenv()

class SimpleQuoter(object):
    @staticmethod
    def sqlquote(x=None):
        return "'bar'"

def lower_first(iterator):
    return itertools.chain([next(iterator).lower()], iterator)

def load_CSV_to_dict(infile, indexname):
    df = pd.read_csv(infile, delimiter=',')
    # Lower case headers
    df.columns = map(str.lower, df.columns)
    df = df.fillna('NoData')
    return df.set_index(indexname, drop=False)

def compare_CSV(tablename):
    SIS_df = load_CSV_to_dict(SIS_FILE.format(table=tablename), 'sisintid')
    Unizin_df = load_CSV_to_dict(UNIZIN_FILE.format(table=tablename),'sisintid')

    print ("Unizin Len:", len(Unizin_df))
    print ("SIS Len", len(SIS_df))

    Unizin_head = list(Unizin_df)
    print (Unizin_head)

    for i, SIS_r in SIS_df.iterrows():
        #Look at all the unizin headers and compare
        sisintid = SIS_r['sisintid'].strip()
        if not sisintid:
            continue
#        print ("Checking sisintid", sisintid)
        try: 
            Unizin_r = Unizin_df.loc[sisintid]
        except:
            #print ("SisIntId not found in Unizin Data CSV",sisintid)
            continue

        for head in Unizin_head:
            try:
                if SIS_r[head] != Unizin_r[head]:
                    print(head," does not match for ",sisintid,SIS_r[head],Unizin_r[head])
            except:
                continue


def load_Unizin_to_CSV(tablename):
    conn = psycopg2.connect(os.getenv("DSN"))
    curs = conn.cursor()

    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER FORCE QUOTE *".format(dbqueries.QUERY[tablename])
    UWriter = open(UNIZIN_FILE.format(table=tablename),"w")
    curs.copy_expert(outputquery, UWriter)

print ("Choose an option.\n1 = load CSV files, 2 = load Unizin Data to CSV (need developer VPN or other connection setup), 3 = Compare SIS CSV and Unizin CSV files (need 1 and 2)")
option = input()

if option == "1":
    compare_CSV('person')
if option == "2":
    load_Unizin_to_CSV('person')
if option == "3":
    print ("Option 3")

sys.exit(0)
