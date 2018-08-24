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

## put in DSN your DSN string

DSN = 'dbname=test'
#SISFile = '2018-08-15%2FPerson_2018-08-15.csv'
SISFile = 'Person-2000.csv'
UnizinFile = "Unizin-Person-2000.csv"

## don't modify anything below this line (except for experimenting)

import sys
import psycopg2
import os
import itertools

import numpy as np
import pandas as pd

from collections import OrderedDict
from operator import itemgetter

class SimpleQuoter(object):
    @staticmethod
    def sqlquote(x=None):
        return "'bar'"

def lower_first(iterator):
    return itertools.chain([next(iterator).lower()], iterator)


from dotenv import load_dotenv
load_dotenv()

def load_CSV_to_dict(infile):
    data = pd.read_csv(infile, delimiter=',')
    # Lower case headers
    data.columns = map(str.lower, data.columns)
    return data

def load_CSV():
    SISDict = load_CSV_to_dict(SISFile)
    UnizinDict = load_CSV_to_dict(UnizinFile)

    SISDict.sort_values(by=['sisintid'])
    UnizinDict.sort_values(by=['sisintid'])
    for i, r in SISDict.iterrows():
        print (UnizinDict.iloc[i]['sisintid'],SISDict.iloc[i]['sisintid'])


def load_Unizin_to_CSV():
    conn = psycopg2.connect(os.getenv("DSN"))

    print("Encoding for this connection is", conn.encoding)

    # SisIntId,SisExtId,FirstName,MiddleName,LastName,Suffix,Sex,Ethnicity,ZipCode,USResidency,HsGpa,ColGpaCum,ActiveDuty,Veteran,EduLevelPaternal,EduLevelMaternal,EduLevelParental,EnrollmentLevel,CourseCount,SatMathPre2016,SatMathPost2016,SatMathCombined,SatVerbalPre2016,SatReadingPost2016,SatVerbalReadingCombined,SatWritingPre2016,SatWritingPost2016,SatWritingCombined,SatTotalCombined,ActReading,ActMath,ActEnglish,ActScience,ActComposite,PhoneNumber,PhoneType,EmailAddress,EmailType

    # CSV Has Prefix Data has Suffix
    # Currently missing Suffix HsGpa ColGpaCum EduLevelPaternal EduLevel Maternal EnrollmentLevelCourseCount Sat*

    query = """SELECT
                ucdmint.sourcekey as SisIntId,
                ucdmext.sourcekey as SisExtId,
                Person.Firstname as FirstName,
                Person.Middlename as MiddleName,
                Person.Lastname as LastName,
                Person.Prefix as Prefix,
                RefSex.description as Sex,
                RefRace.Description as Ethnicity,
                PersonAddress.PostalCode as ZipCode,
                RefUSCitizenshipStatus.Description as UsResidency,
                RefMilitaryActiveStudentIndicator.Description as ActiveDuty,
                RefMilitaryVeteranStudentIndicator.Description as Veteran,
                PersonTelephone.TelephoneNumber as PhoneNumber,
                RefPersonTelephoneNumberType.Description as PhoneType,
                PersonEmailAddress.EmailAddress as EmailAddress,
                RefEmailType.Description as EmailType

                FROM Person
                LEFT JOIN RefSex on RefSex.RefSexId=Person.RefSexId
                LEFT JOIN PersonDemographicRace on PersonDemographicRace.PersonId=Person.PersonId
                LEFT JOIN RefRace on RefRace.RefRaceId=PersonDemographicRace.RefRaceId
                LEFT JOIN PersonAddress on PersonAddress.PersonId=Person.PersonId
                LEFT JOIN RefUSCitizenshipStatus on RefUSCitizenshipStatus.RefUSCitizenshipStatusId=Person.RefUSCitizenshipStatusId
                LEFT JOIN PersonMilitary on PersonMilitary.PersonId=Person.PersonId
                LEFT JOIN RefMilitaryActiveStudentIndicator on RefMilitaryActiveStudentIndicator.RefMilitaryActiveStudentIndicatorId=PersonMilitary.RefMilitaryActiveStudentIndicatorId
                LEFT JOIN RefMilitaryVeteranStudentIndicator on RefMilitaryVeteranStudentIndicator.RefMilitaryVeteranStudentIndicatorId=PersonMilitary.RefMilitaryVeteranStudentIndicatorId
                LEFT JOIN PersonEmailAddress on PersonEmailAddress.PersonId=Person.PersonId
                LEFT JOIN RefEmailType on RefEmailType.RefEmailTypeId=PersonEmailAddress.RefEmailTypeId
                LEFT JOIN PersonTelephone on PersonTelephone.PersonId=Person.PersonId
                LEFT JOIN RefPersonTelephoneNumberType on RefPersonTelephoneNumberType.RefPersonTelephoneNumberTypeId=PersonTelephone.RefPersonTelephoneNumberTypeId
                LEFT JOIN ucdmentitykeymap ucdmint on ucdmint.ucdmkey = Person.PersonId and ucdmint.ucdmentityid = 1 and ucdmint.systemprovisioningid = 1000
                LEFT JOIN ucdmentitykeymap ucdmext on ucdmext.ucdmkey = Person.PersonId and ucdmext.ucdmentityid = 1 and ucdmext.systemprovisioningid = 1001
                ORDER BY SisIntId Asc 
    """

#    query = query + " limit 2000"

    curs = conn.cursor()

    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER FORCE QUOTE *".format(query)
    UWriter = open(UnizinFile,"w") 
#    curs.copy_expert(outputquery, sys.stdout)
    curs.copy_expert(outputquery, UWriter)

def compare_CSV():
    print ("Compare CSV")

print ("Choose an option.\n1 = load CSV files, 2 = load Unizin Data to CSV (need developer VPN or other connection setup), 3 = Compare SIS CSV and Unizin CSV files (need 1 and 2)")
option = input()

if option == "1":
    load_CSV()
if option == "2":
    load_Unizin_to_CSV()
if option == "3":
    compare_CSV()

sys.exit(0)
