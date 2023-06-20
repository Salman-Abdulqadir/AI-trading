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

print(test.compare_by_interval(['timeseries.eod_stock_prices_fmp', 'timeseries.eod_stock_prices_yahoo'], from_date='2020-06-12', to_date='2023-06-13'))