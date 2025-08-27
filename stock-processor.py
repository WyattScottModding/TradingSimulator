import pandas as pd
from datetime import datetime, timedelta
import os
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed

def get_profit(stock_data, symbol):

	print(f"Analyzing stock: {symbol}!")

	highest_price = 0
	buy_price = 0
	profits = []

	for i in range(1, len(stock_data)):
		current_row = stock_data.iloc[i]
		current_price = current_row['vw']
		current_time = current_row['timestamp']

		# Track the highest price seen
		if current_price > highest_price:
			highest_price = current_price

		# Track a 20% price drop
		if current_price < highest_price * 0.8:
			buy_price  = current_price # Execute a buy

		# Track a 20% price rise
		if buy_price > 0 and current_price > buy_price * 1.2:
			sell_price  = current_price # Execute a sell
			profits.append(sell_price / buy_price)
			buy_price = 0 # Reset the buy price
			highest_price = 0 # Reset the highest price seen


	# Calculate total profit
	profit = 1

	for p in profits:
		profit *= p

	return symbol, profit


def get_stock_data(stock_folder, symbol):
	stock_file = os.path.join(stock_folder, f"{symbol}.csv")

	if not os.path.exists(stock_file):
		print(f"Stock file {symbol} does not exist!")
		return None

	try:
		stock_data = pd.read_csv(stock_file)
		stock_data = stock_data[['timestamp', 'vw']].dropna()
		stock_data['timestamp'] = pd.to_datetime(stock_data['timestamp'])
		stock_data.sort_values('timestamp', inplace=True)
		stock_data.reset_index(drop=True, inplace=True)
	except Exception as e:
		print(f"Failed to read {stock_file}: {e}")
		return None
	return stock_data


def analyze_stocks(stock_list_file, stock_data_folder, output_file):

	stock_list = None
	try:
		stock_list = pd.read_csv(stock_list_file)
	except Exception as e:
		print(f"Failed to read {stock_list_file}: {e}")

	futures = []

	# Create the output file and write the header
	with open(output_file, 'w', newline='') as f:
		writer = csv.DictWriter(f, fieldnames=['symbol', 'profit'])
		writer.writeheader()

	with ProcessPoolExecutor(max_workers = 13) as ex:
		# Main thread
		for _, row in stock_list.iterrows():
			symbol = row['symbol']

			stock_data = get_stock_data(stock_data_folder, symbol)

			if stock_data is None: # Don't process stocks with no data
				continue

			futures.append(ex.submit(get_profit, stock_data, symbol)) # Worker thread

		# Main thread
		for fut in as_completed(futures):
			try:
				symbol, profit = fut.result()
				if profit is not None:
					with open(output_file, 'a', newline='') as f: # Append to the file
						writer = csv.DictWriter(f, fieldnames=['symbol', 'profit'])
						writer.writerow( {'symbol': symbol, 'profit': profit} )
			except Exception as e:
				print(f"Caught Exception: {e}")
				continue

if __name__ == "__main__":
	analyze_stocks('mid-cap-stocks.csv', 'Stock-Data', 'results.csv')