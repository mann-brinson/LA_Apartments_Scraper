import neighborhoods_api
import apartments_scrape
import queries_from_terminal
import sys

print(f"We're in file {__file__}")

#Require the user to input this driver and source option
#Will prompt the user to enter a source argument (remote or local)
if len(sys.argv) < 2:
    print('To few arguments, please put in LA_Apartment_Analysis.py and data source argument (remote or local). EX: "LA_Apartment_Analysis.py remote"')
    sys.exit(0)

if sys.argv[1] == 'remote':
	print("Calling neighborhoods_api.py. This should create the neighborhood table. Please wait...")
	neighborhoods_api.main()
	print("Calling apartments_scrape.py. This should create the apartment table. Please wait...")
	apartments_scrape.main()
	print("Calling queries_from_terminal.py. This should return some queries about the database. Please wait...")
	queries_from_terminal.main()

elif sys.argv[1] == 'local':
	print("Calling queries_from_terminal.py. This should return some queries about the database. Please wait...")
	queries_from_terminal.main()

else:
	print("Please enter 'remote' or 'local' as your second argument. EX: 'LA_Apartment_Analysis.py remote' ")
	sys.exit(0)
