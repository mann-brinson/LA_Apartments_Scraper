#!/usr/bin/env python
# coding: utf-8

import argparse
import sys

#GOAL: Create a neighborhood table 
def main():
	import requests
	import json
	import numpy as np
	import pandas as pd
	import os
	import sqlite3
	from datetime import datetime #use for last_modified timestamp

	# DATASOURCE 1: 2015 Neighborhood Tract ID Employment Demographics
	# Source: Los Angeles Office of the Mayor
	# Description: Shows population and employment statistics from 2015 census per tract id
	# within Los Angeles county 

	#Returns all expected 2301 rows, for the 2301 tracts within Los Angeles county
	url = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/Enriched%20United%20States%20Tract%20Boundaries%202015/FeatureServer/0/query?where=COUNTY%20%3D%20'LOS%20ANGELES'&outFields=*&outSR=4326&resultType=standard&f=json"
	r = requests.get(url)
	content = r.content
	data = json.loads(content)
	attr_rows = []
	for feature in data['features']:
	    attr_rows.append(feature['attributes'])
	final_rows = []
	for row in attr_rows:
	    final_rows.append({'fips': row['FIPS'],'population': row['POPULATION'], 'sqmi': row['SQMI'], 'unemployment_rate': row['UNEMPRT_CY']
	                      })
	#Turn the rows into a pandas dataframe
	tract_population = pd.DataFrame(final_rows)

	#Add a year column filled with '2015' because the population count is from 2015
	tract_population['year']='2015'

	#Split the county_code (first 5 digits) from the neighborhood FIPS code (last 6 digits)
	raw_fips_s = tract_population.iloc[:,0]

	# county FIPS code
	county_fips_s = raw_fips_s.str.slice(stop=4)

	# neighborhood FIPS code
	hood_fips_s = raw_fips_s.str.slice(start=5)

	tract_population['fips_hood'] = hood_fips_s
	tract_population.drop(columns =['fips'], inplace = True)

	#DATASOURCE 2: Los Angeles Historic Rent Prices 
	#Source: USC 
	#Description: Shows rent prices from 2010 to 2016 within Los Angeles county
	# for each census tract id
	from sodapy import Socrata

	#Data source: https://usc.data.socrata.com/Los-Angeles/Rent-Price-LA-/4a97-v5tx/data

	# Authenticated client (needed for non-public datasets):
	# NOTE: This is a pulic AppToken with a randomly generated pw
	# WARNING: Only use this App Token within this app
	MyAppToken = '5Vq9Bj2wKGOojNXywvnju8iqO'
	client = Socrata('usc.data.socrata.com',
	                 MyAppToken,
	                 username="mbmann@usc.edu",
	                 password="{eEY7}KY/r2uMZQq")

	# First 17000 results, returned as JSON from API / converted to Python list of
	# dictionaries by sodapy.
	results = client.get("4a97-v5tx", limit=17000)

	# Convert to pandas DataFrame
	results_df = pd.DataFrame.from_records(results)

	# Pull out the relevant columns 
	# Returns all expected 16390 rows
	tract_rent = pd.DataFrame()
	tract_rent = results_df[['tract_number','tract','neighborhood','amount','year']]

	#Clean the columns

	#Split the name column to separate out tract_id, county, and state
	names = tract_rent["tract"].str.split(",", n = 2, expand = True) 
	tract_rent_copy = tract_rent.copy()

	# making separate county column from new data frame 
	tract_rent_copy["county"]= names[1] 
	tract_rent_copy["county"] = tract_rent_copy["county"].str.strip()

	# making separate state column from new data frame 
	tract_rent_copy["state"]= names[2] 
	tract_rent_copy["state"] = tract_rent_copy["state"].str.strip()
	  
	#Dropping old tract column
	tract_rent_copy.drop(columns =["tract"], inplace = True) 
	tract_rent_copy = tract_rent_copy.rename(columns={"amount": "avg_rent", "tract_number": "fips_hood"})

	#Add the fips county code and square miles
	tract_constants = tract_population[['fips_hood', 'sqmi']]
	tract_tmp = pd.merge(tract_rent_copy, tract_constants, how='left', on=['fips_hood'])

	#Hard-code the county fips code, because we are only interested in Los Angeles County
	tract_tmp['fips_county'] = '0603'

	#Left join tract_population on tract_rent dataset where tract_id and year_id equal each other
	tract_population.drop(columns =['sqmi'], inplace = True)
	tract_main_tmp = pd.merge(tract_tmp, tract_population, how='left', on=['fips_hood','year'])

	# Drop missing values
	tract_main_tmp = tract_main_tmp.dropna(subset=['avg_rent'])
	tract_main_tmp = tract_main_tmp.dropna(subset=['sqmi'])

	# Add a 'last_modified' field 
	now = datetime.now()
	last_modified = now.strftime("%D %H:%M:%S")
	tract_main_tmp['last_modified'] = last_modified

	# Hardcode a 'homeless persons' column
	tract_main_tmp['homeless_persons'] = ''

	#DATASOURCE 3: Los Angeles Homelessness
	#Source: USC 
	#Description: Shows rent prices from 2017 to 2018 within Los Angeles county
	# for each census tract id

	#Data source: https://usc.data.socrata.com/Los-Angeles/Homelessness-LA-/e7n7-i6jm

	# Authenticated client (needed for non-public datasets):
	# NOTE: This is a pulic AppToken with a randomly generated pw
	# WARNING: Only use this App Token within this app
	MyAppToken = '5Vq9Bj2wKGOojNXywvnju8iqO'
	client = Socrata('usc.data.socrata.com',
	                 MyAppToken,
	                 username="mbmann@usc.edu",
	                 password="{eEY7}KY/r2uMZQq")

	# First 13000 results, returned as JSON from API / converted to Python list of
	# dictionaries by sodapy.
	results = client.get("e7n7-i6jm", limit=13000)

	# Convert to pandas DataFrame
	results_df = pd.DataFrame.from_records(results)

	# Pull out the relevant columns 
	# Returns all expected 12954 rows
	tract_homeless = pd.DataFrame()
	tract_homeless = results_df[['tractnumber','neighborhood','year', 'variable', 'count']]

	#Select only the rows where variable = 'Total Homeless Population'
	tract_homeless = tract_homeless.loc[tract_homeless['variable'] == 'Total Homeless Population']
	#tract_homeless.head()

	#Hard-code the county fips code, because we are only interested in Los Angeles County
	tract_homeless_copy = tract_homeless.copy()
	tract_homeless_copy = tract_homeless_copy.rename(columns={"count": "homeless_persons", "tractnumber": "fips_hood"})

	#Pull in the sq_mi from the previous tract_constants reference table 
	tract_homeless_tmp = pd.merge(tract_homeless_copy, tract_constants, how='left', on=['fips_hood'])
	#tract_homeless_tmp.head()

	tract_homeless_tmp['fips_county'] = '0603'
	tract_homeless_tmp['county'] = 'Los Angeles County'
	tract_homeless_tmp['state'] = 'California'
	tract_homeless_tmp['avg_rent'] = ''
	tract_homeless_tmp['population'] = ''
	tract_homeless_tmp['unemployment_rate'] = ''

	# Add a 'last_modified' field 
	now = datetime.now()
	last_modified = now.strftime("%D %H:%M:%S")
	tract_homeless_tmp['last_modified'] = last_modified

	# #Reorder the columns to prepare for table union
	tract_homeless_tmp = tract_homeless_tmp[['fips_hood','neighborhood','avg_rent','year', 'county', 'state', 'sqmi', 'fips_county', 'population', 'unemployment_rate', 'last_modified', 'homeless_persons']]

	#Do a table union between 2 dataframes: tract_main_tmp and tract_homeless_tmp
	frames = [tract_main_tmp, tract_homeless_tmp]
	tract_final = pd.concat(frames, ignore_index=True)

	return tract_final

def write_to_db(tract_final):
	import numpy as np
	import pandas as pd
	import os
	import sqlite3
	from datetime import datetime #use for last_modified timestamp

	#NAVIGATE TO THE /data folder
	os.chdir("..")
	os.chdir('/data')					

	#CREATE NEIGHBORHOOD TABLE
	conn = sqlite3.connect('la_apartments.db')
	#conn = sqlite3.connect('/Users/markmann/la_apartments.db')
	cur = conn.cursor()
	cur.execute('DROP TABLE IF EXISTS neighborhood')
	cur.execute('CREATE TABLE neighborhood (last_modified TEXT, id INTEGER PRIMARY KEY, fips_hood INTEGER, neighborhood TEXT, avg_rent INTEGER, year INTEGER, county TEXT, state TEXT, sqmi REAL, fips_county INTEGER, population INTEGER, unemployment_rate REAL, homeless_persons INTEGER)')

	#UPDATE NEIGHORHOOD TABLE
	for i in range(tract_final.shape[0]):
	    cur.execute('INSERT INTO neighborhood (last_modified, fips_hood, neighborhood, avg_rent, year, county, state, sqmi, fips_county, population, unemployment_rate, homeless_persons) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
	                (tract_final['last_modified'][i], tract_final['fips_hood'][i], tract_final['neighborhood'][i], tract_final['avg_rent'][i], tract_final['year'][i], tract_final['county'][i], tract_final['state'][i], tract_final['sqmi'][i], tract_final['fips_county'][i], tract_final['population'][i], tract_final['unemployment_rate'][i], tract_final['homeless_persons'][i]))

	conn.commit()
	conn.close()

	#NAVIGATE BACK TO THE /src folder
	os.chdir("..")
	os.chdir('/src')

if __name__ == '__main__':
    print(f"We're in file {__file__}")
    print("Calling neighborhoods_api.py -> main() ")
    tract_final = main()
    write_to_db(tract_final)
