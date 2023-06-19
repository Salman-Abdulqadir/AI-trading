import yfinance as yf
import pandas as pd
import psycopg2

def insert_data(tickers):
    finance_data = yf.download(tickers, group_by='Ticker' ,period='5y')
    finance_data = finance_data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)

    print("Inserting in progress...")

    for index, row in finance_data.iterrows():
        date = index.to_pydatetime()
        ticker = row['Ticker']
        open_price = row['Open']
        high_price = row['High']
        low_price = row['Low']
        close_price = row['Close']
        volume = row['Volume']
        insert_query = "INSERT INTO timeseries.eod_stock_prices_yahoo (symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cur.execute(insert_query, (ticker, date, open_price, high_price, low_price, close_price, volume))

    connection.commit()
try:
    connection = psycopg2.connect(
        host="192.168.172.24", 
        port="5432",
        database="OHLC", 
        user="saal_user", 
        password="saalAITrade"
    )
except:
    print("Failed to connect to the DB")

cur = connection.cursor()

cur.execute("SELECT symbol FROM stocklist.stock_details");

db_data = cur.fetchall()
tickers = [ticker[0] for ticker in db_data]

i = 50
count = 0
while(i < len(tickers) - 100):
    ticker_interval = tickers[i: i + 100]
    insert_data(ticker_interval)
    i += 100
else:
    ticker_interval = tickers[i::]
    if ticker_interval:
        insert_data(ticker_interval)
        count += len(ticker_interval)

# closing the connection of the db 
connection.commit()
cur.close()
connection.close() 
