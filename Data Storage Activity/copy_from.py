# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv
import pandas as pd

DBname = "census"
DBuser = "postgres1"
DBpwd = "postgres"
TableName = 'censusdata_copyfrom'
Datafile = "acs2015_census_tract_data.csv"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015
tfile = "/home/hariev2/tempfile.csv"


def initialize():
  global Year

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  parser.add_argument("-y", "--year", default=Year)
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable
  Year = args.year


# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection


# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
        	DROP TABLE IF EXISTS {TableName};
        	CREATE TEMP TABLE {TableName} (
            	Year                INTEGER,
                CensusTract         NUMERIC,
            	State               TEXT,
            	County              TEXT,
            	TotalPop            INTEGER,
            	Men                 INTEGER,
            	Women               INTEGER,
            	Hispanic            DECIMAL,
            	White               DECIMAL,
            	Black               DECIMAL,
            	Native              DECIMAL,
            	Asian               DECIMAL,
            	Pacific             DECIMAL,
            	Citizen             DECIMAL,
            	Income              DECIMAL,
            	IncomeErr           DECIMAL,
            	IncomePerCap        DECIMAL,
            	IncomePerCapErr     DECIMAL,
            	Poverty             DECIMAL,
            	ChildPoverty        DECIMAL,
            	Professional        DECIMAL,
            	Service             DECIMAL,
            	Office              DECIMAL,
            	Construction        DECIMAL,
            	Production          DECIMAL,
            	Drive               DECIMAL,
            	Carpool             DECIMAL,
            	Transit             DECIMAL,
            	Walk                DECIMAL,
            	OtherTransp         DECIMAL,
            	WorkAtHome          DECIMAL,
            	MeanCommute         DECIMAL,
            	Employed            INTEGER,
            	PrivateWork         DECIMAL,
            	PublicWork          DECIMAL,
            	SelfEmployed        DECIMAL,
            	FamilyWork          DECIMAL,
            	Unemployment        DECIMAL
         	);	    
            """)

		print(f"Created {TableName}")

def load(conn, icmdlist):

	with conn.cursor() as cursor:
            start = time.perf_counter()
            #print(icmdlist)
            cursor.copy_from(icmdlist, TableName,sep=",")
            elapsed = time.perf_counter() - start
            print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    initialize()
    conn = dbconnect()
    with open(Datafile,'r') as fcon:
        dr = csv.DictReader(fcon)
        header = next(dr)
        rlist =[]
        for r in dr:
            rlist.append(r)
        cmdlist = []
        for row in rlist:
            #print(rlist)
            for key in row:
                if not row[key]:
                    row[key] = 0
                row['County'] = row['County'].replace('\'','')
            cmdlist.append((Year, row['CensusTract'],str(row['State']),str(row['County']),row['TotalPop'],
                                 row['Men'],row['Women'], row['Hispanic'],row['White'],row['Black'],row['Native'],
                                 row['Asian'],row['Pacific'],row['Citizen'],row['Income'], row['IncomeErr'],row['IncomePerCap'],
                                row['IncomePerCapErr'],row['Poverty'],row['ChildPoverty'],row['Professional'],row['Service'],row['Office'],
                                row['Construction'],row['Production'],row['Drive'],row['Carpool'],row['Transit'],row['Walk'],row['OtherTransp'],
                                row['WorkAtHome'],row['MeanCommute'],row['Employed'],row['PrivateWork'],row['PublicWork'],
                                row['SelfEmployed'], row['FamilyWork'], row['Unemployment']))
        df = pd.DataFrame(cmdlist)
        df.to_csv(tfile,index=False,header=False)
        rfile = open(tfile,'r')
    if CreateDB:
    	createTable(conn)

    load(conn, rfile)


if __name__ == "__main__":
    main()




