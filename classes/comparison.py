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
    def generate_query(self, table: str, one_date: str = '', from_date: str = '', to_date:str ='', getall: bool = False):
        query = f"""
            SELECT *
            FROM {table}
        """
        if (one_date != ""):
            query += f" WHERE datetime = '{one_date}'"
        elif (from_date != "" and to_date != ""):
            query += f" WHERE datetime >= '{from_date}' AND datetime <= '{to_date}'"
        elif (not getall):
            query += " LIMIT 200"
        query += " ORDER BY datetime asc"

        return query



    def connect_to_db(self) -> object:
        connection = psycopg2.connect(
            host = self.db_cred["host"],
            port = self.db_cred["port"],
            database = self.db_cred["database"],
            user = self.db_cred["user"],
            password = self.db_cred["password"]
            )
        return connection
    
    def compare_by_interval(self, tables: list[str], from_date:str, to_date: str) -> None:
        try:
            connection = self.connect_to_db()
            cursor = connection.cursor()

            columns = ["symbol", "datetime", "open", "high", "low", "close", "volume"]

            print("Fetching table 1 from DB...")

            cursor.execute(self.generate_query(table=tables[0], from_date=from_date, to_date=to_date))
            table1 = cursor.fetchall()
            df1 = pd.DataFrame(table1, columns=columns)

            print("\nTable 1 fetched successfully!\n\nFetching table 2 from DB...\n")

            cursor.execute(self.generate_query(table=tables[1], from_date=from_date, to_date=to_date))
            table2 = cursor.fetchall()
            df2 = pd.DataFrame(table2, columns=columns)

            print("Table 2 fetched successfully!")

            merged = pd.merge(df1, df2, on=["symbol", "datetime"], how="outer")
            merged.set_index(["symbol", "datetime"], inplace=True)

            print(merged)
        except:
            print("Something wrong happened!")
    
    def compare_one_date(self, tables: list[str], one_date: str) -> None:
        try:
            if not self.is_valid_date(one_date) or not self.db_cred:
                raise ValueError ("Date is not valid or DB info is empty")

            connection = self.connect_to_db()
            cursor = connection.cursor()

            print("Fetching table 1 from DB...")
            
            cursor.execute(self.generate_query(tables[0], one_date = one_date))
            table1 = cursor.fetchall()

            print("Table 1 fetched successfully!\nFetching table 2 from DB...")

            cursor.execute(self.generate_query(tables[1], one_date = one_date))
            table2 = cursor.fetchall()

            print("Table 2 fetched successfully!")
                  
            columns = ["symbol", "datetime", "open", "high", "low", "close", "volume"]
            df1 = pd.DataFrame(table1, columns=columns)
            df2 = pd.DataFrame(table2, columns=columns)

            # saving the data to a data frame
            merged = pd.merge(df1, df2, on=["symbol", "datetime"], how="outer")
            merged.set_index([ "symbol", "datetime"], inplace=True)

            
            # print(f"File saved!\npath: ./comparison_log/{date}_comparison.csv\n")
            cursor.close()
            connection.close()

            print(merged)
          
        except (ValueError, KeyError):
            print(ValueError)
    
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

    