import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime, timedelta
import time

# Get URL
url = 'https://stockanalysis.com/list/mid-cap-stocks/'

response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find('table')

headers = [header.text for header in table.find_all('th')]
rows = table.find_all('tr')[1:] # Skip the header rows

# Extract the stock information
stock_list = []
for row in rows:
	columns = row.find_all('td')
	symbol = columns[1].text
	name = columns[2].text
	market_cap = columns[3].text
	revenue = columns[6].text
	stock_list.append({'symbol': symbol, 'name': name, 'market_cap': market_cap, 'revenue': revenue})

df = pd.DataFrame(stock_list)
#print(df)
#df.to_csv('mid-cap-stocks.csv')


# Get stock data
api_keys = [
    'API_KEY1',
    'API_KEY2',
    'API_KEY3'
]

api_key_index = 0

def get_current_api_key():
	global api_key_index
	return api_keys[api_key_index]

def switch_api_key():
	global api_key_index
	api_key_index = (api_key_index + 1) % len(api_keys)

save_dir = 'Stock-Data'
os.makedirs(save_dir, exist_ok=True)

# Get the list of stock symbols we just downloaded
symbols_df = pd.read_csv(f'mid-cap-stocks.csv', delimiter=',')

for symbol in symbols_df['symbol']:
	print(f"Starting data retrieval for {symbol}...")

	csv_filename = os.path.join(save_dir, f'{symbol}.csv')

	if os.path.exists(csv_filename):
		print("File already exists - skipping")
	else:
		# Define the datarange of data to grab
		end_date = datetime.now()
		start_date = end_date - timedelta(days=365*2) # two years

		all_data = pd.DataFrame()
		current_start = start_date

		while current_start < end_date:
			current_end = min(current_start + timedelta(days=150), end_date) # With the free version of the api there is a limit to the date range
			print(f"Fetching data from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

			# Polygon api endpoint
			url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{current_start.strftime('%Y-%m-%d')}/{current_end.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&limit=150000&apiKey={get_current_api_key()}"

			while True:
				response = requests.get(url)
				if response.status_code == 200:
					data = response.json().get('results', [])
					if data:
						df = pd.DataFrame(data)

						df['timestamp'] = pd.to_datetime(df['t'], unit='ms')

						df = df.drop(columns=['t'])

						all_data = pd.concat([all_data, df])
						break
					else:
						print(f"No data returned for {symbol} from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
						break
				elif response.status_code == 429: # Rate limit exceeded
					print(f"Rate limit exceeded for API key. Switching to next key...")
					switch_api_key()
					time.sleep(5) # Wait 5 seconds before retrying
				else:
					print(f"Failed to retrieve data for {symbol} from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}: {response.status_code} {response.text}")
					time.sleep(60) # Wait 60 seconds before retrying

			current_start = current_end + timedelta(days=1)

		all_data.reset_index(inplace=True, drop=True)
		all_data.to_csv(csv_filename, index=False)
		print(f"Data fetching complete for {symbol}. Data saved to {csv_filename}")

print("All data fetching complete!")