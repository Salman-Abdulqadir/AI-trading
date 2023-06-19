import psycopg2
import yfinance as yf

connection = psycopg2.connect(
    host="192.168.172.24", 
    port="5432",
    database="OHLC", 
    user="saal_user", 
    password="saalAITrade"
)

cur = connection.cursor()

def get_finance_data(ticker):
    data = yf.download(ticker, period="48y", progress=False)
    return data

def populateDB(ticker):
    try:
        finance_data = get_finance_data(ticker)
    # Insert finance data into the database
        for index, row in finance_data.iterrows():
            date = index.to_pydatetime()
            open_price = row['Open']
            high_price = row['High']
            low_price = row['Low']
            close_price = row['Close']
            volume = row['Volume']
            insert_query = "INSERT INTO timeseries.eod_stock_prices_yahoo (symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cur.execute(insert_query, (ticker, date, open_price, high_price, low_price, close_price, volume))
        connection.commit()
        return True
    except:
         return False


# getting the tickers from the database
cur.execute("SELECT symbol FROM stocklist.stock_details");
tickers = [ ticker[0] for ticker in cur.fetchall()]

success = open("successfully_inserted.txt", "a")
fail = open("not_inserted.txt", "a")

for ticker in tickers:
    if ticker == "AAPL":
        continue
    status = populateDB(ticker)
    if status:
        success.write(ticker + "\n")
        print(f"Data for f{ticker} inserted successfully!")
    else:
        fail.write(ticker + "\n")
        print(f"Failed to insert")

# Commit the changes and close the connection
connection.commit()
cur.close()
connection.close()

print("Finance data inserted into the database.")
