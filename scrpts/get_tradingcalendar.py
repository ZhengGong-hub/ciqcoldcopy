import pandas_market_calendars as mcal
import os

nyse = mcal.get_calendar('NYSE')

tcalendar = nyse.schedule(start_date='1990-01-01', end_date='2066-07-10').reset_index()
tcalendar.rename(columns = {'index': 'tradingday'}, inplace = True)


print(tcalendar)
os.makedirs('data/tradingcalendar', exist_ok=True)
tcalendar.to_csv('data/tradingcalendar/tradingcalendar.csv', index=False)