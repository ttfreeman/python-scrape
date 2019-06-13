import requests
from bs4 import BeautifulSoup

res = requests.get("https://www.nasdaq.com/symbol/labd/option-chain")
soup = BeautifulSoup(res.content,'lxml')
table = soup.find_all('table')[0] 
df = pd.read_html(res.content)[2]
print(df.to_json(orient='records'))
 

df.to_csv('xle_options.csv')
print(df.head())