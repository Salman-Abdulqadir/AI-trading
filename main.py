from classes import comparison


db = {
    "host":"192.168.172.24", 
    "port":"5432",
    "database":"OHLC", 
    "user":"saal_user", 
    "password":"saalAITrade"
}

precision = 0.001
test = comparison.Comparison(db)

print(test.rows_for_symbols(['timeseries.eod_stock_prices_fmp', 'timeseries.eod_stock_prices_yahoo'], '2019-01-01', '2023-01-01'))