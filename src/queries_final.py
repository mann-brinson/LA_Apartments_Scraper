#!/usr/bin/env python
# coding: utf-8

import argparse
import sys

import sqlite3
import pandas as pd
import os
import shutil
import matplotlib.pylab as pylab
import matplotlib.pyplot as plt
import seaborn as sns

#GOAL: Run queries on la_apartments.db to return simple metrics about the database

def check_db():
  '''
  Initial function checks that the db exists before proceeding with queries.
  If the db tables exist, will return the cursor object
  '''
  #Connect to the database
  conn = sqlite3.connect('la_apartments.db')
  cur = conn.cursor()

  #Check to see that the neighborhood table exists before continuing
  sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='neighborhood'"
  cur.execute(sql)
  response = cur.fetchall()
  if len(response) == 0:
      print("The neighborhood table doesn't exist. You must first source the data remotely.")
      sys.exit(0)

  #Check to see that the apartment table exists before continuing
  sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='apartment'"
  cur.execute(sql)
  response = cur.fetchall()
  if len(response) == 0:
      print("The apartment table doesn't exist. You must first source the data remotely.")
      sys.exit(0)
  return cur

def query1():
	cur = check_db()
	#How many rows are in the neighborhood table?
	sql = ('SELECT COUNT(*) '
	     'FROM neighborhood ')
	cur.execute(sql)
	response = cur.fetchall()
	return 'neighborhood table rows:', response[0][0]

def query2():
	cur = check_db()
	#How many rows are in the apartment table?
	sql = ('SELECT COUNT(*) '
	     'FROM apartment ')
	cur.execute(sql)
	response = cur.fetchall()
	return 'apartment table rows:', response[0][0]

def query3():
	cur = check_db()
	#What are the prices, bedrooms, and bathrooms of the 5 apartments with lowest square footage 
	sql = ('SELECT craigslist_id, sq_feet, price, bedrooms, bathrooms '
	      'FROM '
	          '(SELECT * '
	          'FROM apartment '
	          'ORDER BY sq_feet asc) apartment '
	      'LIMIT 5 ')
	cur.execute(sql)
	response = cur.fetchall()
	response_df = pd.DataFrame(response, columns=['craigslist_id', 'sq_feet', 'price', 'bedrooms', 'bathrooms'])
	print('Top 5 Smallest Apartments:') 
	print(response_df)

def query4():
	cur = check_db()
	#What are the 5 Los Angeles census tracts, and its neighborhood that had the highest rent in 2015?
	sql = ('SELECT fips_hood, neighborhood, year, avg_rent, sqmi, population '
	      'FROM '
	          '(SELECT * '
	          'FROM neighborhood '
	          'WHERE year = 2015 '
	          'ORDER BY avg_rent desc) neighborhood '
	      'LIMIT 5 ')
	cur.execute(sql)
	response = cur.fetchall()
	response_df = pd.DataFrame(response, columns=['tract_id', 'neighborhood', 'year', 'avg_rent', 'sqmi', 'population'])
	print('Top 5 Most Expensive Census Tracts:') 
	print(response_df)	

def query5():
	cur = check_db()
	#Which neighborhood tracts have the most homeless people? 
	sql = ('SELECT year, fips_hood, neighborhood, homeless_persons '
	      'FROM neighborhood '
	      'WHERE neighborhood.year = 2018 '
	      'ORDER BY homeless_persons desc '
	      'LIMIT 5')
	cur.execute(sql)
	response = cur.fetchall()
	response_df = pd.DataFrame(response, columns=['year', 'tract', 'neighborhood', 'homeless_persons'])
	print('Top 5 tracts with the most homeless persons:') 
	print(response_df)

def query6():
	cur = check_db()
	#What apartments had the lowest price per square foot, and in what neighborhoods? 
	sql = ('SELECT neighborhood, price_per_sqfoot, price, sq_feet, bedrooms, bathrooms, url '
	     'FROM '
	         '(SELECT tract_id, bedrooms, bathrooms, substr(price,2) price, sq_feet, round((CAST(substr(price,2) AS FLOAT)/sq_feet), 2) price_per_sqfoot, url '
	         'FROM apartment '
	         'ORDER BY price_per_sqfoot asc) price_per_sqfoot '
	     'JOIN neighborhood on price_per_sqfoot.tract_id = neighborhood.fips_hood '
	     'WHERE year = 2016 '
	     'ORDER BY price_per_sqfoot asc '
	     'LIMIT 5'
	    )
	cur.execute(sql)
	response = cur.fetchall()
	response_df = pd.DataFrame(response, columns=['neighborhood', 'price_per_sqfoot', 'price', 'sq_feet', 'bedrooms', 'bathrooms', 'url'])
	print('Top 5 Best Value Per Square Foot Apartments:') 
	print(response_df)

def query7():
	cur = check_db()
	#For the apartments, which apartments are in neighborhoods with the least amount of homeless people? 
	sql = ('SELECT apartment.id, apartment.bedrooms, apartment.price, neighborhood.fips_hood, neighborhood.neighborhood, neighborhood.year, neighborhood.homeless_persons '
	     'FROM apartment '
	     'LEFT JOIN neighborhood on apartment.tract_id = neighborhood.fips_hood '
	     'WHERE neighborhood.year = 2018 '
	     'ORDER BY neighborhood.homeless_persons asc '
	     'LIMIT 5'
	  )
	cur.execute(sql)
	response = list(cur.fetchall())
	response_df = pd.DataFrame(response, columns=['apt_id', 'bedrooms', 'price', 'tract', 'neighborhood', 'year', 'homeless_persons'])
	print('Top 5 Apartments with the Least Homeless Persons Nearby:') 
	print(response_df)

def query8():
	cur = check_db()
	#For the apartments, which apartments are in neighborhoods with the most amount of homeless people? 
	sql = ('SELECT apartment.id, apartment.bedrooms, apartment.price, neighborhood.fips_hood, neighborhood.neighborhood, neighborhood.year, neighborhood.homeless_persons '
	     'FROM apartment '
	     'LEFT JOIN neighborhood on apartment.tract_id = neighborhood.fips_hood '
	     'WHERE neighborhood.year = 2018 '
	     'ORDER BY neighborhood.homeless_persons desc '
	     'LIMIT 5'
	  )
	cur.execute(sql)
	response = list(cur.fetchall())
	response_df = pd.DataFrame(response, columns=['apt_id', 'bedrooms', 'price', 'tract', 'neighborhood', 'year', 'homeless_persons'])
	print('Apartments with the Most Homeless Persons Nearby:') 
	print(response_df)

def query9():
	cur = check_db()
	#Which neighborhood tracts in Hollywood that have the least homeless people per square mile? 
	sql = ('SELECT fips_hood, neighborhood, year, homeless_persons, sqmi, round((homeless_persons/sqmi),0) homeless_per_sqmi '
	      'FROM neighborhood '
	      'WHERE year = 2018 AND neighborhood = "Hollywood" AND homeless_persons > 0 AND sqmi != "None" '
	    'ORDER BY homeless_per_sqmi asc '
	    'LIMIT 5')
	cur.execute(sql)
	response = list(cur.fetchall())
	response_df = pd.DataFrame(response, columns=['tract', 'neighborhood', 'year', 'homeless_persons', 'sqmi', 'homeless_per_sqmi'])
	print('Tracts in Hollywood with the least homeless per square mile:') 
	print(response_df)

def query10():
	cur = check_db()
	#What apartments are in neighborhoods with the least homeless per sqmi? 
	sql = ('SELECT apartment.id, apartment.bedrooms, apartment.price, neighborhood.fips_hood, neighborhood.neighborhood, neighborhood.year, round((neighborhood.homeless_persons/neighborhood.sqmi),0) homeless_per_sqmi '
	     'FROM apartment '
	     'LEFT JOIN neighborhood on apartment.tract_id = neighborhood.fips_hood '
	     'WHERE neighborhood.year = 2018 AND homeless_persons > 0 AND sqmi != "None" '
	     'ORDER BY homeless_per_sqmi asc '
	     'LIMIT 5'
	  )
	cur.execute(sql)
	response = list(cur.fetchall())
	response_df = pd.DataFrame(response, columns=['apt_id', 'bedrooms', 'price', 'tract', 'neighborhood', 'year', 'homeless_per_sqmi'])
	print('Apartments in tracts with the least homeless per square mile:') 
	print(response_df)