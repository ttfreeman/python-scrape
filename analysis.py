import pandas as pd

df = pd.read_csv('internet_users.csv')

df2 = df[:5].drop(['GRAPH', 'HISTORY', '#', 'Unnamed: 0'], axis=1)

df2=df2.dropna(how='all', axis='columns')

print(df2.to_json(orient='records'))