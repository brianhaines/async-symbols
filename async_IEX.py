import sqlite3
import datetime
import asyncio
import concurrent.futures
import requests
import pandas_datareader as pdr
import pandas as pd
import numpy as np

def get_IEX(symbol='AAPL'):
	#start = datetime.datetime(year=2013, month=3, day=15)
	#end   = datetime.datetime(year=2018, month=3, day=15)
	end = datetime.date.today()
	start = end - datetime.timedelta(days=365.25*5)
	t = pdr.DataReader(symbol, 'iex', start, end)
	return t

def add_atr_to_dataframe(dataframe):
    dataframe['ATR1'] = abs(dataframe['high'] - dataframe['low'])
    dataframe['ATR2'] = abs(dataframe['high'] - dataframe['close'].shift())
    dataframe['ATR3'] = abs(dataframe['low'] - dataframe['close'].shift())
    dataframe['TrueRange'] = dataframe[['ATR1', 'ATR2', 'ATR3']].max(axis=1)
    return dataframe


async def run_IEX():
	from finsymbols import symbols
	
	#create or connect to existing db
	db = sqlite3.connect('stock_tables.db')
	
	# Get the symbol list from wiki
	all_symbols = symbols.get_sp500_symbols()

	# Longer lists of symbols are available
	# all_symbols = symbols.get_nyse_symbols() 3,139
	# all_symbols = symbols.get_nasdaq_symbols() 3,295
	# all_symbols = symbols.get_amex_symbols() 315

	
	# Google and Fox have dual class shares so drop superfluous symbols
	drop_symbols = ['GOOG', 'FOX']
	good_symbols = [x['symbol'] for x in all_symbols if x['symbol'] not in set(drop_symbols)]
	# good_symbols = good_symbols[480:] # Turn this on for testing...only runs last ~20 tickers

	# Save symbols and their data to db
	symbol_frame = pd.DataFrame(all_symbols)
	symbol_frame.to_sql('Names',db)

	executor = concurrent.futures.ThreadPoolExecutor(max_workers=50)
	
	futures = [loop.run_in_executor(executor, get_IEX, i) for i in good_symbols]
	results = await asyncio.gather(*futures)

	for (i, result) in zip(good_symbols, results):
		p = result.iloc[-1]['close']
		last = result.index.values[-1]
		first = result.index.values[0]
		#print(results)
		print('{0}: {1} close = {2} - First day available {3}'.format(last, i, p, first))
		
		# Add return columns
		# https://stackoverflow.com/questions/31287552/logarithmic-returns-in-pandas-dataframe
		result['pct_change'] = result.close.pct_change()
		result['log_return'] = np.log(1 + result['pct_change'])

		# Add ATR to the frame too
		result = add_atr_to_dataframe(result)

		# Save to table with symbol as the name
		result.to_sql(i, db)
	
	# Commit DB
	db.commit()
	# Close the DB connection	
	db.close()



if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(run_IEX())