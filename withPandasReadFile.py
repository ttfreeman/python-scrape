import pandas as pd

file = open('page1.html', 'r')
if file.mode =='r':
    content = file.read()

dfs = pd.read_html(content)
for df in dfs:
    print(df)
dfs[2].to_csv('xle_options.csv')
print(df.head())