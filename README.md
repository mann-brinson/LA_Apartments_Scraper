# LA Apartment & Neighborhood Scraper and Analytics

**USER GUIDE**

**DESCRIPTION:** This application will create and run queries against an apartment and neighborhood database for Los Angeles rental housing options. 

**HOW TO RUN THE PYTHON APPLICATION**

STEP 1: Install Anaconda Python 3.7
	Link: https://www.anaconda.com/distribution/

STEP 2: Download the zip file 'INF510_Project-master.zip' locally

STEP 3: Withn your terminal, navigate to the repository, and then into /src

STEP 4: Install the required python modules with the following command

```$ pip install --user --requirement REQUIREMENTS.txt```

STEP 5: Run the program with the following command options 
```
$ python3 LA_Apartment_Analysis.py local
$ python3 LA_Apartment_Analysis.py remote
```

STEP 6: Finally, run the 'mann_mark.ipynb' in Jupyter Notebook to view scatterplots and answers to assignment questions.  

**LOCAL AND REMOTE OPTIONS**

**local** - runs the program with the local database, if one is present. If the local database is not present, you will need to run the program remotely, to generate a local database before the analysis (aka queries) can be completed. 

**remote** - runs the program after first creating a database from remote web sources within 'neighborhoods_api.py' and 'apartments_scrape.py'.


**PROGRAM FILES**

**1. 'LA_Apartment_Analysis.py'** - calls other program files for database creation, and queries. Can be run with data pulled locally or remotely.
**2. 'neighborhoods_api.py'** - creates the Los Angeles neighborhood db table.
**3. 'apartments_scrape.py'** - creates the Los Angeles apartment db table.
**4. 'la_apartments.db'** - database file that will be created from 3. and 4. above. Example db is provided if you immediately want to run the program with local data. 
**5. 'queries_from_terminal.py'** - runs queries against the db. To be used within the terminal only
**6. 'mann_mark.ipynb'** - the Jupyter notebook containing answers to questions about the final assignment for USC INF 510, and visualizations. 
**7. 'queries_final.py'** - called from the Jupyter notebook in #6 to see queries from within notebook
**8. 'scatterplots_final.py'** - called from the Jupyter notebook in #6 to see scatterplots from within notebook 
