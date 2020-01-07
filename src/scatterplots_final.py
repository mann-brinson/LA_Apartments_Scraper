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

#GOAL: Run scatterplots on la_apartments.db to return simple metrics about the database

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

def scatterplot1():
  cur = check_db()
  #Show me a scatterplot of price (X) versus square-feet for all apartments
  sql = ('SELECT id, bedrooms, bathrooms, substr(price,2) price, sq_feet, round((CAST(substr(price,2) AS FLOAT)/sq_feet), 2) price_per_sqfoot, url '
          'FROM apartment '
          'WHERE sq_feet < 3000')
  cur.execute(sql)
  response = list(cur.fetchall())
  apt_price_sqfeet = pd.DataFrame(response, columns=['id', 'bedrooms', 'bathrooms', 'price', 'sq_feet', 'price_per_sqfoot', 'url'])

  #Export a csv file with this dataframe, for inspection
  export_csv = apt_price_sqfeet.to_csv('scatterplot_bed_bath_sqfeet.csv', index = None, header=True)
  apt_price_sqfeet['price'] = apt_price_sqfeet['price'].astype(int)

  params = {'legend.fontsize': 'x-large',
          'figure.figsize': (10, 7),
         'axes.labelsize': 'x-large',
         'axes.titlesize':'x-large',
         'xtick.labelsize':'x-large',
         'ytick.labelsize':'x-large'}
  pylab.rcParams.update(params)
  sns.scatterplot(x='price', y='sq_feet', hue='bedrooms', hue_order=['0BR','1BR','2BR','3BR'], palette='summer', data=apt_price_sqfeet.dropna())
  plt.title('Apartments Within 3 Miles of Pan Pacific Park');
  plt.xlabel('Price');
  plt.ylabel('Square Feet');

def scatterplot2():
  cur = check_db()
  #Show a scatterplot of an apartment's tract's homeless per square mile (X) 
  # and apartment price (Y)
  sql = ('SELECT apartment.id, apartment.bedrooms, substr(price,2) price, neighborhood.fips_hood, neighborhood.neighborhood, neighborhood.year, round((neighborhood.homeless_persons/neighborhood.sqmi),0) homeless_per_sqmi '
           'FROM apartment '
           'LEFT JOIN neighborhood on apartment.tract_id = neighborhood.fips_hood '
           'WHERE neighborhood.year = 2018 AND homeless_persons > 0 AND sqmi != "None"'
        )
  cur.execute(sql)
  response = list(cur.fetchall())
  apt_price_homeless = pd.DataFrame(response, columns=['id', 'bedrooms', 'price', 'tract_id', 'neighborhood', 'year', 'homeless_per_sqmi'])
  export_csv = apt_price_homeless.to_csv('scatterplot_homeless_per_sqmi.csv', index = None, header=True) 
  apt_price_homeless['price'] = apt_price_homeless['price'].astype(int)

  params = {'legend.fontsize': 'x-large',
          'figure.figsize': (10, 7),
         'axes.labelsize': 'x-large',
         'axes.titlesize':'x-large',
         'xtick.labelsize':'x-large',
         'ytick.labelsize':'x-large'}
  pylab.rcParams.update(params)
  sns.scatterplot(x='homeless_per_sqmi', y='price', data=apt_price_homeless.dropna())
  plt.title('Apartments Within 3 Miles of Pan Pacific Park');
  plt.xlabel('Homeless Persons per Square Mile');
  plt.ylabel('Price');

def scatterplot3():
  cur = check_db()
  #Show me a combination of scatterplot 1 and scatterplot 2
  sql = ('SELECT apt_id, tract_id, neighborhood, year, bedrooms, price, sq_feet, price_per_sqfoot, homeless_persons, sqmi, round((neighborhood.homeless_persons/neighborhood.sqmi),0) homeless_per_sqmi, url ' 
            'FROM '
                '(SELECT id apt_id, tract_id, bedrooms, substr(price,2) price, sq_feet, round((CAST(substr(price,2) AS FLOAT)/sq_feet), 2) price_per_sqfoot, url '
                'FROM apartment '
                'WHERE sq_feet < 3000) price_per_sqft '
            'LEFT JOIN neighborhood on price_per_sqft.tract_id = neighborhood.fips_hood '
            'WHERE neighborhood.year = 2018 AND sqmi != "None" AND price_per_sqfoot < 20')

  cur.execute(sql)
  response = list(cur.fetchall())
  apt_ppsqft_hpsqmi = pd.DataFrame(response, columns=['apt_id', 'tract_id', 'neighborhood', 'year', 'bedrooms', 'price', 'sq_feet', 'price_per_sqfoot', 'homeless_persons', 'sqmi', 'homeless_per_sqmi', 'url'])

  #Shows the quartiles (min, 25, median, 75, max) of each quantitative attribute
  stats_table = apt_ppsqft_hpsqmi.describe()

  # Create a label called 'ppsqft_low_YN'
  # Set it to 'Yes' if the price_per_sqfoot is less than 25% quartile
  ppsqft_low_list = []
  for row in apt_ppsqft_hpsqmi['price_per_sqfoot']:
      if row <= stats_table['price_per_sqfoot']['50%']:
          ppsqft_low_list.append(1)
      else:
          ppsqft_low_list.append(0)

  # Create a label called 'hpsqmi_low_YN'
  # Set it to 'Yes' if the homeless_per_sqmi is less than 25% quartile
  hpsqmi_low_list = []
  for row in apt_ppsqft_hpsqmi['homeless_per_sqmi']:
      if row <= stats_table['homeless_per_sqmi']['50%']:
          hpsqmi_low_list.append(1)
      else:
          hpsqmi_low_list.append(0)

  apt_ppsqft_hpsqmi_v2 = apt_ppsqft_hpsqmi.copy()
  apt_ppsqft_hpsqmi_v2['ppsqft_low_YN'] = ppsqft_low_list
  apt_ppsqft_hpsqmi_v2['hpsqmi_low_YN'] = hpsqmi_low_list

  export_csv = apt_ppsqft_hpsqmi_v2.to_csv('scatterplot_apt_ppsqft_hpsqmi_v2.csv', index = None, header=True) 

  # Create a label of labels, labelling apartment as 'Great Deal', 'Average Deal', 'Poor Deal'
  # 'Great Deal' if there is both a low 'price_per_sqfoot' and 'homeless_per_sqmi'
  # 'Average Deal' if one of the two metrics is low
  # 'Poor Deal' if none of the two metrics are low
  under_50q_list = []
  for index, row in apt_ppsqft_hpsqmi_v2.iterrows():
      if row['ppsqft_low_YN'] == 1 and row['hpsqmi_low_YN'] == 1:
          under_50q_list.append('Both')
      elif row['ppsqft_low_YN'] == 1 and row['hpsqmi_low_YN'] == 0:
          under_50q_list.append('One')
      elif row['ppsqft_low_YN'] == 0 and row['hpsqmi_low_YN'] == 1:
          under_50q_list.append('One')
      else:
          under_50q_list.append('None')

  apt_ppsqft_hpsqmi_v3 = apt_ppsqft_hpsqmi_v2.copy()
  apt_ppsqft_hpsqmi_v3['under_50th_quartile'] = under_50q_list

  export_csv = apt_ppsqft_hpsqmi_v3.to_csv('scatterplot_apt_ppsqft_hpsqmi_v3.csv', index = None, header=True) 

  apt_ppsqft_hpsqmi_v3['price_per_sqfoot'] = apt_ppsqft_hpsqmi_v3['price_per_sqfoot'].astype(float)
  apt_ppsqft_hpsqmi_v3['homeless_per_sqmi'] = apt_ppsqft_hpsqmi_v3['homeless_per_sqmi'].astype(float)

  params = {'legend.fontsize': 'x-large',
          'figure.figsize': (10, 7),
         'axes.labelsize': 'x-large',
         'axes.titlesize':'x-large',
         'xtick.labelsize':'x-large',
         'ytick.labelsize':'x-large'}
  pylab.rcParams.update(params)
  sns.scatterplot(x='homeless_per_sqmi', y='price_per_sqfoot', hue='under_50th_quartile', hue_order=['None','One','Both'], data=apt_ppsqft_hpsqmi_v3.dropna())
  plt.title('Apartments Within 3 Miles of Pan Pacific Park');
  plt.xlabel('Homeless persons per square mile');
  plt.ylabel('Price per Square Foot');  

