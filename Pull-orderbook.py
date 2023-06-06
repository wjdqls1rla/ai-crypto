import sys
from datetime import datetime
import json
import csv
import os
import requests
import time
import pandas as pd
import argparse
import timeit
import urllib3
import numpy as np 
import collections
import matplotlib as plt
from easydict import EasyDict
import atexit

def handle_exit():
    ## runs when program is shut
    print((datetime.now()).strftime('%Y-%M-%D %H:%M:%S'))


def default_settings(): 
    ##Setting crypto market & crypto currency after run

    setting = EasyDict({
        'market' : str(input("select market :" )),
        'currency' : str(input("select currency: "))
    })

    #validation
    while (setting.market not in market_list) or (setting.currency not in currency_list):

        if setting.market not in market_list:
            print(market_list)
            print("Unavailable market. Please try other market above")
            setting.market = input("select market: " )

        if setting.currency not in currency_list:
            print(currency_list)
            print("Unavailable currency. Please try other currency above")
            setting.currency = input("select currency: " )

    return setting

def error_message(timestamp, type):
    File = open('./raw/error_log.txt', 'a')
    if type == 0:
        File.write("An orderbook error has occurred in " + timestamp.strftime('%Y-%m-%d %H:%M:%S') + "\n")
    else:

        File.write("An tradebook error has occurred in " + timestamp.strftime('%Y-%m-%d %H:%M:%S') + "\n")
    File.close()

def write_csv(data, timestamp):
    date = timestamp.strftime('%Y-%m-%d')

    #File name: Date_market_orderbook.csv
    csv_directory = ["./raw/" + date + "-" + market + '-' + currency + "-orderbook.csv", 
                     "./raw/" + date + "-" + market + '-' + currency + "-tradebook.csv"]

    if not os.path.isdir('./raw'):
        os.makedirs('./raw')

    #Save header when file doesn't exists
    for i in {0, 1}:
        should_write_header = os.path.exists(csv_directory[i])
        if should_write_header == False:
            data[i].to_csv(csv_directory[i], index=False, header=True, mode = 'a')
        else:
            data[i].to_csv(csv_directory[i], index=False, header=False, mode = 'a')

def get_orderbook_tradebook(market, url_dictionary, timestamp):

    #request orderbook
    try: 
        orderbook_response = requests.get(url_dictionary[market][0]).json()
        orderbook = orderbook_response['data']
    except:
        error_message(timestamp, 0)
        orderbook = None

    #request tradebook
    try: 
        tradebook_response = requests.get(url_dictionary[market][1]).json()
        tradebook = tradebook_response['data']
    except:
        error_message(timestamp, 1)
        tradebook = None

    return orderbook, tradebook
    
def reprocess_orderbook(orderbook, request_time):

    ##bid = buy
    bids = (pd.DataFrame(orderbook['bids'])).apply(pd.to_numeric, errors='ignore')
    bids.sort_values('price', ascending=False, inplace=True) ##ascending
    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0

    ##ask = sell
    asks = (pd.DataFrame(orderbook['asks'])).apply(pd.to_numeric, errors='ignore')
    asks.sort_values('price', ascending=True, inplace=True) ##descending
    asks = asks.reset_index(); del asks['index']
    asks['type'] = 1

    #rearranged orderbook
    new_orderbook = pd.concat([bids, asks])
    new_orderbook['quantity'] = new_orderbook['quantity'].round(decimals=4)
    new_orderbook['timestamp'] = request_time
    
    """
    new_orderbook format:
            Price             |   quantity   |type |  timestamp
    Bid 1 : Lowest price      |   quantity   |  0  |  timestamp
    Bid 2 : 2nd lowest price  |   quantity   |  0  |  timestamp
        ...  :        ...        |      ...     | ... |     ...
    Bid n : Highest price     |   quantity   |  0  |  timestamp
    Ask 1 : Highest price     |   quantity   |  1  |  timestamp
    Ask 2 : 2nd highest price |   quantity   |  1  |  timestamp
        ...  :        ...        |      ...     | ... |     ...
    Ask n :Lowest price       |   quantity   |  1  |  timestamp
    """
    
    return new_orderbook

def reprocess_tradebook(tradebook):
    
    global previous_tradebook

    tradebook = pd.DataFrame(tradebook).apply(pd.to_numeric, errors='ignore')

    #change type to 0 and 1
    try:
        tradebook.loc[tradebook['type'] == 'bid', 'type'] = 0
        tradebook.loc[tradebook['type'] == 'ask', 'type'] = 1 
    except:
        tradebook = previous_tradebook
        
    tradebook.sort_values('transaction_date', ascending = True, inplace = True) ## ascending
    
    if previous_tradebook.empty:
        previous_tradebook = tradebook
        return tradebook
    
    #extract new trade information rows only
    test = pd.merge(previous_tradebook, tradebook, how = 'outer', indicator = True)
    test = test.query("_merge == 'right_only'")
    test = test.drop(columns = ['_merge'])

    previous_tradebook = tradebook
    tradebook = test

    return tradebook

def pull_csv_orderbook_tradebook():
    
    timestamp = datetime.now()
    time_interval = 0

    while(1):
        # setting pull time interval
        time_interval = datetime.now() - timestamp
        if (time_interval.total_seconds() < 1):
            continue

        timestamp = datetime.now()
        request_time = timestamp.strftime('%Y-%M-%D %H:%M:%S')

        orderbook, tradebook = get_orderbook_tradebook(market, url_dictionary, timestamp)

        orderbook = reprocess_orderbook(orderbook, request_time)
        tradebook = reprocess_tradebook(tradebook)

        data = [orderbook, tradebook]

        write_csv(data, timestamp)

market_list = ['Bithumb', 'Upbit' ]
currency_list = ['BTC', 'ETH', 'USDT']

def main():

    global market
    global currency 
    global url_dictionary
    global previous_tradebook

    previous_tradebook = pd.DataFrame()
    initial_setting = default_settings()
    market = initial_setting["market"]
    currency = initial_setting["currency"]

    url_dictionary = {'Bithumb':['https://api.bithumb.com/public/orderbook/%s_KRW/?count=10' %currency, 
                                 'https://api.bithumb.com/public/transaction_history/%s_KRW/?count=50' % currency], 
                      'Upbit':['https://api.upbit.com/v1/orderbook?markets=KRW-%s' % currency,
                                ]}

    pull_csv_orderbook_tradebook()

main()
