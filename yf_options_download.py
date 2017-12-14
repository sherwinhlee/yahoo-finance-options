"""
Script to download 
options data for 
specified ticker.
Source: Yahoo Finance
"""

from urllib.request import urlopen
import simplejson
import sys
import csv
import datetime
import os
import pg8000

# Global constants
ticker = input("Enter the ticker: ") 				     # ticker input
write_table = [["option_id", "quotedate", "expiration",  # init empty table/headers
			"strike", "contract", "type", "last_trade", 
			"bid", "ask", "volume", "open_interest", 
			"implied_vol"]]
			
def dir():
	"""
	Returns pathname
	of directory.
	"""
	if getattr(sys, 'frozen', False):
		# frozen
		filepath = os.path.realpath(sys.executable)
	else:
		# unfrozen
		filepath = os.path.realpath(__file__)

	# directory name	
	return os.path.dirname(filepath)

def pulldates():
	"""
	Extracts pull date.
	Returns tuple:
	out[0] = YYYY-MM-DD
	out[1] = YYMMDD
	"""
	pull = datetime.date.today()
	pull_str = str(pull).replace("-","")[2:]
	return pull, pull_str

def generate_url(ticker, cdate="", date=True):
	"""
	Function generates a 
	URL to Yahoo Finance 
	JSON file of specified 
	ticker.
	params: ticker, contract date (optional)
	out = URL (str) 
	"""
	
	# URL constants
	s1 = 'https://query2.finance.yahoo.com/v7/finance/options/'
	s2 = '?formatted=true&crumb=H0ZsGkp7jUn&lang=en-US&region=US'
	s3 = '&corsDomain=finance.yahoo.com'
	
	# Date-inclusive URL or generic URL
	if date:
		return s1 + ticker + s2 + '&date=' + cdate + s3
	else:
		return s1 + ticker + s2 + s3

def main():

	# Open generic JSON URL from Yahoo Finance
	ticker_json_url = generate_url(ticker, date=False)
	request = urlopen(ticker_json_url)
	ticker_json = simplejson.load(request)

	# Parse list of contract dates
	dates = ticker_json["optionChain"]["result"][0]["expirationDates"]

	# Initialize page counter
	page_counter = 0

	# Loop through contract date pages
	for timestamp in dates:

		# Convert unix timestamp to date
		date = (datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d'))

		# Load data on contract date page
		date_url  = generate_url(ticker, str(timestamp))
		request = urlopen(date_url)
		date_json = simplejson.load(request)
		
		"""
		Following section parses data for call options.
		"""
		
		# Variables for call and put objects
		calls = date_json["optionChain"]["result"][0]["options"][0]["calls"]
		puts = date_json["optionChain"]["result"][0]["options"][0]["puts"]

		# Initialize flag for publishing/passing on date page
		pass_flag = False
		
		# If call options exist for specified date	
		try:
			for opt in range(len(calls)):
			
				# Sets contract name 
				contract = calls[opt]["contractSymbol"]
				
				if contract[:3] == ticker:
				# Option matches specified ticker
				
					# Initialize empty row
					call_row = []
					
					# Parse call options data
					strike = calls[opt]["strike"]["raw"]
					last_trade = calls[opt]["lastPrice"]["raw"]
					bid = calls[opt]["bid"]["raw"]
					ask = calls[opt]["ask"]["raw"]
					change = calls[opt]["change"]["raw"]
					pct_change = calls[opt]["percentChange"]["raw"]
					volume = calls[opt]["volume"]["raw"]
					open_interest = calls[opt]["openInterest"]["raw"]
					implied_vol = calls[opt]["impliedVolatility"]["raw"]
					option_id = contract + pulldates()[1]
					
					# Append cells to call row
					call_row.extend([option_id, pulldates()[0], date, \
									strike, contract, "call", last_trade, \
									bid, ask, volume, open_interest, implied_vol])
					
					# Append row to table
					write_table.append(call_row)
					
				else:
				# Passes if option does not match specified ticker
					pass_flag = True
					pass
				
		# Exception for when parsing call options throws error
		except:
			pass
		
		"""
		Following section parses data for put options.
		"""
		
		# If put options exist for specified date	
		try:
			for opt in range(len(puts)):
			
				# Sets contract name
				contract = puts[opt]["contractSymbol"]
				
				if contract[:3] == ticker:
				# Option matches specified ticker
					
					# Initialize empty row
					put_row = []
					
					# Parse put options data
					strike = puts[opt]["strike"]["raw"]
					last_trade = puts[opt]["lastPrice"]["raw"]
					bid = puts[opt]["bid"]["raw"]
					ask = puts[opt]["ask"]["raw"]
					change = puts[opt]["change"]["raw"]
					pct_change = puts[opt]["percentChange"]["raw"]
					volume = puts[opt]["volume"]["raw"]
					open_interest = puts[opt]["openInterest"]["raw"]
					implied_vol = puts[opt]["impliedVolatility"]["raw"]
					option_id = contract + pulldates()[1]	
					
					# Append cells to put row
					put_row.extend([option_id, pulldates()[0], date, \
									strike, contract, "put", last_trade, \
									bid, ask, volume, open_interest, implied_vol])
					
					# Append row to table
					write_table.append(put_row)
					
					# Keep pass flag at false
					pass_flag = False
				
				else:
				# Passes if option is not specified ticker
					pass_flag = True
					pass
				
		# Exception for when parsing call options throws error
		except:
			pass
			
		if pass_flag == False:
			print("Processed date page: ", date)
			
			# Page counter
			page_counter += 1
			
	# CSV holding directory
	outputDir = dir() + '/csv/'

	# Name output file
	outputFname = ticker + "_" + str(pulldates()[0]) + ".csv"

	# Full output path + file names, #2 converts io.TextIOWrapper obj to str
	outputFile = str(outputDir) + outputFname
	outputFile2 = outputFile.replace("\\","/")

	# Write table rep to .csv file
	with open(outputFile, 'w') as outputFile:
		write_obj = csv.writer(outputFile, lineterminator='\n')
		write_obj.writerows(write_table)
		
	print("Options data for " + str(page_counter) + \
		  " dates successfully scraped!")

# Call main function
if __name__ == '__main__':
	main()