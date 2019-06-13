import pandas as pd
from google.cloud import firestore
from google.cloud import storage
from datetime import datetime

# API clients
gcs = None
db = None

data = {'bucket':'jupez-n-scraped-html','name': '2019-06-10T23:53:44.924Zxlf'}

def analyze_html(data):
    scraped_html = 'https://www.nasdaq.com/symbol/xle/etf-detail'
    analysis_result = parse_html(data, scraped_html)
    docref_list = persist(analysis_result, data['name'])
    print('Created new Firestore documents',
                len(docref_list))



# def get_gcs_file_contents(data):
#   """Get the content of the GCS object that triggered this function."""
#   global gcs
#   if not gcs:
#     gcs = storage.Client()
#   bucket = gcs.get_bucket(data['bucket'])
#   blob = bucket.blob(data['name'])
#   return blob.download_as_string()

def persist(analysis_result, collection_id):
    # print('analysis_result: ', analysis_result['latest_options'])
    global db
    insert_list=[]
    if not db:
        db = firestore.Client()
    collection_name = str(collection_id)+'-scrape-analysis'
    for option in analysis_result['latest_options']:
        collection = db.collection(collection_name)
        document_id = str(option['symbol'])+str(option['expirationDate'])+str(option['strikePrice'])
        inserted = collection.add(option, document_id)
        insert_list.append(inserted[1])
    return insert_list
    # [END main-block]


# [START parse-block]
def parse_html(data, html):
  """Parse the supplied HTML and return a dict with details of the operation."""
  gcs_filename = 'gs://{}/{}'.format(data['bucket'], data['name'])
  parse_result = {'input_file': gcs_filename,
                  'analysis_timestamp': datetime.utcnow().isoformat() + 'Z'}
  options = extract_data(html)
  if options:
    parse_result['status'] = 'SUCCESS'
    parse_result['latest_options'] = options
  else:
    logging.warning('FAILED analysis of %s', gcs_filename)
    parse_result['status'] = 'FAILED'
  return parse_result


def extract_data(html):

  dfs = pd.read_html(html, header=None)
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
    
  print(df.head())
  print('shape of dataframe=', df.shape)
  return df.to_dict(orient='records')

analyze_html(data)

# dfs = pd.read_html("https://www.nasdaq.com/symbol/xlf/option-chain", header=None)

# for df in dfs:
#     print(df)
# dfs[2].columns = [ 
#     "expirationDate",
#     "call-closingPrice",
#     "call-chg",
#     "call-bid",
#     "call-ask",
#     "call-volume",
#     "call-openInterest",
#     "symbol",
#     "strikePrice",
#     "put-expirationDate",
#     "put-closingPrice",
#     "put-chg",
#     "put-bid",
#     "put-ask",
#     "put-volume",
#     "put-openInterest"
# ]   

# df = dfs[2].drop(columns=['call-chg', 'put-expirationDate', 'put-chg'])
    
# df.to_csv('xle_options.csv')
# print(df.head())
# df.to_json('resuts.json', orient='records')