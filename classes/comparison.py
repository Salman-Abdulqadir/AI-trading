import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime

class Comparison:
    def __init__(self, db_cred: dict) -> None:
        self.db_cred = db_cred

    def set_db_cred(self, db_cred: dict) -> None:
        self.db_cred = db_cred
    
    # check if a given date is valid
    def is_valid_date(self, date_string: str) -> bool:
        try:
            if date_string == "":
                return True
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    # generating query that gets data from DB
    def generate_query(self, tables: list[str], date: str):
        return f"""
            SELECT
                {tables[0]}.symbol,
                {tables[0]}.datetime,
                round({tables[0]}.open, 4),
                round({tables[0]}.close, 4),
                round({tables[0]}.high, 4),
                round({tables[0]}.low,4),
                round({tables[1]}.open,4),
                round({tables[1]}.close,4),
                round({tables[1]}.high,4),
                round({tables[1]}.low, 4)
            FROM {tables[0]}
            INNER JOIN {tables[1]}
                ON {tables[0]}.datetime = {tables[1]}.datetimE and {tables[0]}.symbol = {tables[1]}.symbol
            WHERE {tables[0]}.datetime = '{date}'
            ORDER BY {tables[0]}.datetime asc, {tables[0]}.symbol asc, {tables[1]}.symbol asc
        """

    def connect_to_db(self) -> object:
        connection = psycopg2.connect(
            host = self.db_cred["host"],
            port = self.db_cred["port"],
            database = self.db_cred["database"],
            user = self.db_cred["user"],
            password = self.db_cred["password"]
            )
        return connection
    
    def compare_by_date(self, date: str, tables: list[str]) -> None:
        try:
            if not self.is_valid_date(date) or not self.db_cred:
                raise ValueError ("Date is not valid or DB info is empty")

            connection = self.connect_to_db()
            cursor = connection.cursor()

            cursor.execute(f"select count(*) from {tables[0]} where datetime = '{date}'")

            print(f"""
                    **********************************************
                    Fetching {cursor.fetchall()[0][0]} entries...
                    **********************************************\n
                """)
            
            cursor.execute(self.generate_query(tables, date))

            # saving the data to a data frame
            dfs = self.save_to_df(cursor.fetchall())

            # saving the mismatched entries to a csv file
            mismatched_entries = dfs["table1"].compare(dfs["table2"], keep_equal=False)
            mismatched_entries.to_csv(f"comparison_log/{date}_comparison.csv", sep ="\t")
            
            print(f"File saved!\npath: ./comparison_log/{date}_comparison.csv\n")
            cursor.close()
            connection.close()

            print(mismatched_entries)
            return 
          

        except (ValueError, KeyError):
            print(ValueError)

    def save_to_df(self, fetcheddata) -> dict:
        columns = ["symbol", "datetime", "open", "close", "high", "low", "open1", "close1", "high1", "low1"]
        renamedColumns = {
            "open1": "open",
            "close1": "close",
            "high1": "high",
            "low1": "low",
        }
        all = pd.DataFrame(fetcheddata, columns=columns)
        all.set_index(["symbol", "datetime"], inplace=True)
        table1 = all[["open", "close", "high", "low"]]
        table2 = all.drop(["open", "close", "high", "low"], axis=1)
        table2.rename(columns=renamedColumns, inplace=True)

        return {"table1": table1, "table2": table2}
    
    def rows_for_symbols(self, tables: list, from_date: str, to_date:str):
        connection = self.connect_to_db()
        cursor = connection.cursor()

        query = """
            SELECT symbol, count(*) 
            FROM {table} 
            WHERE datetime >= '{from_date}'
            AND datetime <= '{to_date}'
            GROUP BY symbol
        """

        cursor.execute(query.format(table = tables[0], from_date = from_date, to_date = to_date))
        fmp = pd.DataFrame(cursor.fetchall(), columns=["symbol","fmp #rows"])

        cursor.execute(query.format(table = tables[1], from_date = from_date, to_date = to_date))
        yahoo = pd.DataFrame(cursor.fetchall(), columns=["symbol","yahoo #rows"])
        
        merged = pd.merge(fmp, yahoo, on="symbol", how="outer")
        merged.set_index("symbol", inplace=True)

        mismatching_rows = (merged["fmp #rows"] != merged["yahoo #rows"] ).sum()
        yahoo_null =  merged["yahoo #rows"].isnull().sum()
        fmp_null = merged["fmp #rows"].isnull().sum()

        merged["fmp #rows"].fillna(0, inplace=True)
        merged["yahoo #rows"].fillna(0, inplace=True)
        merged["Error %"] = np.where(merged["fmp #rows"] != 0,
                                     abs(merged["fmp #rows"] - merged["yahoo #rows"])/ (merged["fmp #rows"]) * 100,
                                     abs(merged["fmp #rows"] - merged["yahoo #rows"])/ (merged["yahoo #rows"]) * 100)
        merged.to_csv("merged.csv", sep="\t")

        print(merged)

        return f"""
            Total number of symbols: {len(merged.index)}\n
            Mismatching Count: {mismatching_rows}\n
            Present in fmp null in yahoo: {yahoo_null}\n
            Present in yahoo null in fmp: {fmp_null}
        """

    