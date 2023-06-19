f = open("remarks.txt", 'r')


data = f.readlines()
missing_tickers = [line[line.index("[") + 2:line.index("]") - 1] for line in data]
missing_tickers.sort();

with open("missing_info.txt", 'w') as f:
    for ticker in missing_tickers:
        f.write(ticker + "\n")

    
f.close()