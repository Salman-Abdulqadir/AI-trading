import yfinance as yf
import psycopg2
import pandas

class DownloadCSV:
    def __init__(self, db: dict, tickerdb: str = "stocklist.stock_details", period: str = "2y") -> None:
        self.db: dict = db
        self.period = period
        self.tickerdb = tickerdb
        self.tickers = []
        self.connection = {}
        self.get_tickers()

    def set_db (self, db: dict) -> None:
        self.db = db

    def get_db(self) -> dict:
        return self.db

    # connecting to the database
    def connect_db(self) -> object:
        try:
            self.connection = psycopg2.connect(
                host=self.db["host"], 
                port=self.db["port"],
                database=self.db["database"], 
                user=self.db["user"], 
                password=self.db["password"]
            )
        except(TypeError, KeyError):
            print ("Database information is not correct")

    # getting tickers from database
    def get_tickers(self) -> None:
        self.connect_db()
        cur = self.connection.cursor()
        # getting the tickers from the db
        cur.execute(f"SELECT symbol FROM {self.tickerdb}");

        db_data = cur.fetchall()
        self.tickers = [ticker[0] for ticker in db_data]
        cur.close()
        self.connection.close()
        
    # downloading files based on tickers
    def download_data(self) -> None:
        i = 0
        while(i < len(self.tickers) - 1000):
            ticker_interval = self.tickers[i: i + 1000]
            finance_data = yf.download(ticker_interval, group_by='Ticker' ,period=self.period)
            finance_data = finance_data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
            finance_data.to_csv(f"{self.tickers[i]}-{self.tickers[i + 999]}.csv")
            i += 1000
        else:
            ticker_interval = self.tickers[i::]
            finance_data = yf.download(ticker_interval, group_by='Ticker' ,period=self.period)
            finance_data = finance_data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
            finance_data.to_csv(f"{self.tickers[i]}-{self.tickers[-1]}.csv")
                 
db = {
    "host":"192.168.172.24", 
    "port":"5432", 
    "database":"OHLC",
    "user":"saal_user", 
    "password":"saalAITrade"
    }

test = DownloadCSV(db=db, period='48y')
test.download_data()