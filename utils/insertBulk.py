import pandas as pd
import psycopg2
from datetime import datetime

var = pd.read_csv("./data/data(XFIN - last .csv")

try:
    connection = psycopg2.connect(
        host="192.168.172.24", 
        port="5432",
        database="OHLC", 
        user="saal_user", 
        password="saalAITrade"
    )
    print("Connected succesfully")
except:
    print("db connection failed")


cur = connection.cursor()

def get_tickers (data) -> list:
    tickers = []
    for key in data:
        if data[key][0] in tickers or key == "Date":
            continue
        tickers.append(data[key][0])
    return tickers

def get_dates(data) -> list:
    dates = []
    for date in data:
        if str(date) != "nan":
            date_format = '%Y-%m-%d %H:%M:%S'
            date_obj = datetime.strptime(str(date), date_format)
            dates.append(date_obj)
    return dates

tickers = get_tickers(var)
dates = get_dates(var["Date"])


for i in range(len(dates)):
    date = dates[i]
    for j in range(len(tickers)):
        ticker = tickers[j]
        if(j == 0):
            open_price = var["Open"][i+2]
            close_price = var["Close"][i+2]
            high_price = var["High"][i+2]
            low_price = var["Low"][i+2]
            volume = var["Volume"][i+2]
        else:
            open_price = var[f"Open.{j}"][i+2]
            close_price = var[f"Close.{j}"][i+2]
            high_price = var[f"High.{j}"][i+2]
            low_price = var[f"Low.{j}"][i+2]
            volume = var[f"Volume.{j}"][i+2]

        insert_query = "INSERT INTO timeseries.eod_stock_prices_yahoo (symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cur.execute(insert_query, (ticker, date, open_price, high_price, low_price, close_price, volume))
    connection.commit()
    



connection.commit()
cur.close()
connection.close()
print()



