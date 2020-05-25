import requests
import json
import os
import os.path
import time
import pandas as pd

stocks_path = 'jsons/stocks.json'
metrics_path = 'jsons/metrics/{}.json'
balance_path = 'jsons/balance/{}.json'
valuation_path = 'jsons/valuation/{}.json'

def create_dir():
    if not os.path.exists('jsons'):
        os.makedirs('jsons')

    if not os.path.exists('jsons/metrics'):
        os.makedirs('jsons/metrics')

    if not os.path.exists('jsons/balance'):
        os.makedirs('jsons/balance')

    if not os.path.exists('jsons/valuation'):
        os.makedirs('jsons/valuation')


def load_json(filename):
    try:
        with open(filename) as f:
            return json.load(f)
    except:
        with open(filename.replace('.json', '_.json')) as f:
            return json.load(f)

def save(json, filename):
    try:
        with open(filename, 'w') as f:
            f.write(json)
    except:
        with open(filename.replace('.json', '_.json'), 'w') as f:
            f.write(json)

def download(force=False):
    create_dir()

    if force or not os.path.exists(stocks_path):
        #download todos os códigos
        request = requests.get('https://financialmodelingprep.com/api/v3/company/stock/list')
        stocks_str = json.dumps(request.json())
        save(stocks_str, stocks_path)    
    
    stocks = load_json(stocks_path)['symbolsList']
    i = 0
    for stock in stocks:
        symbol = stock['symbol']
        done = False

        while(not done):

            try:
                if force or (not os.path.exists(metrics_path.format(symbol)) and not os.path.exists(metrics_path.format(symbol + '_'))):
                    url_metrics = 'https://financialmodelingprep.com/api/v3/company-key-metrics/{}?period=quarter'.format(symbol)
                    request = requests.get(url_metrics)
                    metrics_str = json.dumps(request.json().get('metrics') or [])
                    save(metrics_str, metrics_path.format(stock['symbol']))

                if force or (not os.path.exists(balance_path.format(symbol)) and not os.path.exists(balance_path.format(symbol + '_'))):
                    url_balance = 'https://financialmodelingprep.com/api/v3/financials/balance-sheet-statement/{}?period=quarter'.format(symbol)
                    request = requests.get(url_balance)
                    balance_str = json.dumps(request.json().get('financials') or [])
                    save(balance_str, balance_path.format(stock['symbol']))

                if force or (not os.path.exists(valuation_path.format(symbol)) and not os.path.exists(valuation_path.format(symbol + '_'))):
                    url_valuation = 'https://financialmodelingprep.com/api/v3/enterprise-value/{}?period=quarter'.format(symbol)
                    request = requests.get(url_valuation)
                    valuation_str = json.dumps(request.json().get('enterpriseValues') or [])
                    save(valuation_str, valuation_path.format(stock['symbol']))

                if(i % 1000 == 0):
                    print('Download {} index {} ...'.format(symbol, i))

                done = True
            
            except ConnectionError:
                print('Sleep... 60 seconds | index {} ...'.format(i))
                time.sleep(60)


        i += 1
        
def load_dataframe(filename):
    stock = load_json(filename)
    df = pd.DataFrame.from_dict(stock, orient='columns').set_index('date')   
    return df; 
            





    
    
