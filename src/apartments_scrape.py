#!/usr/bin/env python
# coding: utf-8

import argparse
import sys

#GOAL: Scrape craigslist apartments and create an apartment table 
def main():
    import requests
    from requests.exceptions import RequestException
    from warnings import warn
    from bs4 import BeautifulSoup
    import numpy as np
    import pandas as pd
    import json
    from datetime import datetime #Used for last_modified timestamp
    from time import sleep #Avoid rate limiting
    import random
    import os
    import sqlite3

    def get_link_list(url_query):
        ''' Generates a list of html objects containing the embedded apartment links
        INPUT: url_query - the url containing the apartment search criteria
        OUTPUT: a list of html objects containing the embedded apartment links
        '''
        r = requests.get(url_query, 'html.parser')
        apt_list_soup = BeautifulSoup(r.text, 'html.parser')

        #Get a list of apartment links from apartment search results
        #NOTE: This shows all link results, regardless of how many partial results appear on page 1
        a_list = apt_list_soup.find_all('a')

        #Make a list of links to visit, to gether details. Each link will become a row in apartment table
        link_list = []
        for a_item in a_list:
            #Try to pull out the link from 'href' attribute if the first element of a_item has 'class' attribute equal to 'result-title'
            try:
                if a_item.attrs['class'][0] == 'result-title':
                    link_list.append(a_item.attrs['href'])
            except KeyError:
                continue
        return link_list

    def process_links(link_list):
        ''' Takes in a list of links, and creates a table of one row per link
        INPUT: link_list - list of links
        OUPUT: pd dataframe with one row per link
        '''
        rows = []
        #Go into each link, and get row details
        for link in link_list:
            headers = {'Accept': 'text/html'}
            try:
                r = requests.get(link, headers=headers)
            except RequestException as exc:
                pass
            #sleep(random.random()) #Waits for a random time between 0 and 1 seconds
            #throw warning for status codes that are not 200
            if r.status_code != 200:
                warn('Request: {}; Status code: {}'.format(requests, r.status_code))
            apt_soup = BeautifulSoup(r.text, 'html.parser')
            #LAST MODIFIED
            now = datetime.now()
            last_modified = now.strftime("%D %H:%M:%S")
            #CRAIGSLIST ID
            p_list = apt_soup.find_all('p')
            cl_id_list = []
            for p_item in p_list:
                try:
                    if p_item.attrs['class'][0] == 'postinginfo':
                        cl_id_list.append(p_item)
                except KeyError:
                    continue
            try:
                cl_id = cl_id_list[1].string[9:]
            except IndexError:
                continue
            #CREATED
            p_list = apt_soup.find_all('p')
            cl_id_list = []
            for p_item in p_list:
                #From p_list pull out p_item that has attribute class = 'postinginfo'
                try:
                    if p_item.attrs['class'][0] == 'postinginfo':
                        cl_id_list.append(p_item)
                except KeyError:
                    continue
            created = str(cl_id_list[2].find('time').string) + ':00'
            #NAME
            meta_list = apt_soup.find_all('meta')
            cl_name = ''
            for meta_item in meta_list:
                try:
                    if meta_item.attrs['property'] == 'og:title':
                        cl_name = meta_item.attrs['content']
                except KeyError:
                    continue
            #PRICE
            span_list = apt_soup.find_all('span')
            cl_price = ''
            for span_item in span_list:
                try:
                    if span_item.attrs['class'][0] == 'price':
                        cl_price = span_item.string
                except KeyError:
                    continue
            #LATITUDE AND LONGITUDE
            for meta_item in meta_list:
                try:
                    if meta_item.attrs['name'] == 'geo.position':
                        lat_long = meta_item.attrs['content']
                except KeyError:
                    continue
            latitude, longitude = lat_long.split(';')
            #BEDROOM, BATHROOM, & SQUARE FEET
            bedbath_list = []
            for span_item in span_list:
                try:
                    if span_item.attrs['class'][0] == 'shared-line-bubble':
                        bedbath_list.append(span_item)
                except KeyError:
                    continue
            #BEDROOMS
            try:
                bedbath_list2 = bedbath_list[0].find_all('b')
                bedrooms = bedbath_list2[0].string
            except IndexError:
                bedrooms = 0
                continue
            #BATHROOMS
            try:
                bedbath_list2 = bedbath_list[0].find_all('b')
                bathrooms = bedbath_list2[1].string
            except IndexError:
                bathrooms = 0
                continue
            #SQ_FEET
            try:
                sq_feet = bedbath_list[1].b.string
            except IndexError:
                sq_feet = 0
                continue
            rows.append({"LAST_MODIFIED": last_modified, "CRAIGSLIST_ID": cl_id, "CREATED": created, "NAME": cl_name, "PRICE": cl_price, 
                     "LATITUDE": latitude, "LONGITUDE": longitude, "BEDROOMS": bedrooms,
                     "BATHROOMS": bathrooms, "SQ_FEET": sq_feet, "URL": link})
        return rows

    def request_tractid(apt_tractid_url_list):
        '''
        Takes in the a list of census api urls and returns the relevant json object that has apartment 
        tract_id. If the json object is an error message, try the request again recursively
        '''
        apt_tractid_list = []
        #Go into each link, and get row details
        for apt_tractid_url in apt_tractid_url_list:
            try:
                r = requests.get(apt_tractid_url, timeout=3)
            except RequestException as exc:
                apt_tractid_list.append(0)
                continue
            except Timeout:
                apt_tractid_list.append(0)
                continue
            content = r.content
            data = json.loads(content)
            try:
                tract_id = data['result']['geographies']['Census Tracts'][0]['TRACT']
                apt_tractid_list.append(tract_id)
            except KeyError:
                apt_tractid_list.append(0)
                continue
        return apt_tractid_list

    #DEFAULT APARTMENT SEARCH CRITERIA

    #Description: Searchs for Los Angeles apartments with a picture, posted today with rent price 
    # between $1800-2800. Also search for apartment within 3 miles of zip code 90036 (Pan Pacific Park)
    # Sort the apartments showing the most recent on top.

    # If run remotely, you will usually you will get around 100-300 rows.

    #NOTE: Eventually will make these search filters available for user to set in an interface, 
    # so they can create their own customized LA apartments database. 

    sort = 'date'
    bundleDuplicates = '1'
    hasPic = '1'
    max_price = '2800'
    min_price = '1800'
    postal = '90036'
    postedToday = '1'
    search_distance = '3'

    #DEFAULT APARTMENT SEARCH URL
    #Posted today (takes about 5 minutes when run remotely...)
    url_query = f'https://losangeles.craigslist.org/search/apa?sort={sort}&availabilityMode=0&bundleDuplicates={bundleDuplicates}&hasPic={hasPic}&max_price={max_price}&min_price={min_price}&postal={postal}&postedToday={postedToday}&search_distance={search_distance}'
    link_list = get_link_list(url_query)

    #Posted anytime (takes about 1 hour to run remotely...)
    # pageitems = 0
    # initial_url = 'https://losangeles.craigslist.org/search/apa?sort=date&hasPic=1&bundleDuplicates=1&search_distance=3&postal=90036&min_price=1800&max_price=2800&availabilityMode=0&sale_date=all+dates'
    # links = get_link_list(initial_url)
    # len_links = len(links)
    # link_list = []
    # while len_links > 0:
    #     for i in links:
    #         link_list.append(i)
    #     pageitems += 120
    #     url = f'https://losangeles.craigslist.org/search/apa?s={pageitems}&availabilityMode=0&bundleDuplicates=1&hasPic=1&max_price=2800&min_price=1800&postal=90036&search_distance=3&sort=date'
    #     links = get_link_list(url)
    #     len_links = len(links)

    #NOTE: With the Craigslist scraping, we will take what we can get. It is not important
    # for us to grab all possible listings. If one apartment attribute cannot be scraped, we will
    # move on and try to scrape the next apartment. This is why we are implementing lots 
    # of exception handling when doing the requests.

    #KNOWN ISSUES
    #ISSUE 1: When running a list of links through process_links(), we don't output a row for
    # every result. But we can do about 90% or more of all apartment results.

    #ISSUE 2: We are getting some requests exceptions sometimes.... Ex: SSLError, 
    # ChunkedEncodingError. This is why we are using RequestException module

    #NOTE: On average process_links() will take around 3 minutes. However, this may take up to 8 minutes 
    # to run, due to the random 1 sec sleep, and the max apartments craigslist search 
    # can return is 3000. 
    rows = process_links(link_list)

    #Turn the dictionary into a dataframe
    apartments = pd.DataFrame(rows) 

    #Get the tract id for each apartment
    #NOTE: request_tractid() can take up to 8 minutes as well. To upgrade, look into python task concurrency
    #Make a list of tract_id urls to fetch
    apt_tractid_url_list = []
    for i in range(apartments.shape[0]):
        longitude = apartments['LONGITUDE'][i]
        latitude = apartments['LATITUDE'][i]
        tractid_url = f'https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={longitude}&y={latitude}&benchmark=Public_AR_Current&vintage=Current_Current&format=json'
        apt_tractid_url_list.append(tractid_url)

    #Call the request_tractid() function to get apartment tract_ids
    apt_tractid_list = request_tractid(apt_tractid_url_list)

    #Add the apt_tractid to the apartment dataframe
    apartments['TRACT_ID'] = apt_tractid_list

    #NAVIGATE TO THE /data folder
    os.chdir("..")
    os.chdir('data')

    #CREATE APARTMENT TABLE
    conn = sqlite3.connect('la_apartments.db')
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS apartment')
    cur.execute('CREATE TABLE apartment (last_modified TEXT, id INTEGER PRIMARY KEY, craigslist_id INTEGER, created TEXT, name TEXT, price TEXT, bedrooms TEXT, bathrooms TEXT, sq_feet INTEGER, tract_id INTEGER, latitude REAL, longitude REAL, url TEXT)')

    #UPDATE APARTMENT TABLE
    for i in range(apartments.shape[0]):
        cur.execute('INSERT INTO apartment (last_modified, craigslist_id, created, name, price, bedrooms, bathrooms, sq_feet, tract_id, latitude, longitude, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (apartments['LAST_MODIFIED'][i], apartments['CRAIGSLIST_ID'][i], apartments['CREATED'][i], apartments['NAME'][i], apartments['PRICE'][i], apartments['BEDROOMS'][i], apartments['BATHROOMS'][i], apartments['SQ_FEET'][i], apartments['TRACT_ID'][i], apartments['LATITUDE'][i], apartments['LONGITUDE'][i], apartments['URL'][i]))
    conn.commit()
    conn.close()

    #NAVIGATE BACK TO THE /src folder
    os.chdir("..")
    os.chdir('src')

if __name__ == '__main__':
    print(f"We're in file {__file__}")
    print("Calling apartments_scrape.py -> main() ")
    main()