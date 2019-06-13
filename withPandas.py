import pandas as pd

dfs = pd.read_html("https://www.nasdaq.com/symbol/xlf/option-chain", header=None)
# for df in dfs:
#     print(df)
dfs[2].columns = [ 
    "expirationDate",
    "call-closingPrice",
    "call-chg",
    "call-bid",
    "call-ask",
    "call-volume",
    "call-openInterest",
    "symbol",
    "strikePrice",
    "put-expirationDate",
    "put-closingPrice",
    "put-chg",
    "put-bid",
    "put-ask",
    "put-volume",
    "put-openInterest"
]   

df = dfs[2].drop(columns=['call-chg', 'put-expirationDate', 'put-chg'])
    
df.to_csv('xle_options.csv')
print(df.head())
df.to_json('resuts.json', orient='records')