import pandas as pd
import numpy as np
import os


def write_csv(data, filename): #filename format: 'YYYY-MM-DD-Market-Currency-FeatureName.csv'

    csv_directory = "./raw/features/" + filename

    if not os.path.isdir('./raw/features'):
        os.makedirs('./raw/features')

    #Save header when file doesn't exists
    should_write_header = os.path.exists(csv_directory)

    if should_write_header == False:
        data.to_csv(csv_directory, index=False, header=True, mode = 'a')
    else:
        data.to_csv(csv_directory, index=False, header=False, mode = 'a')

def read_orderbook(filename):

    data = pd.read_csv(filename).apply(pd.to_numeric, errors = 'ignore')
    grouped_data = data.groupby(['timestamp'])
    group_n = len(grouped_data)
    level = grouped_data.size().iloc[0]/2

    return data, level, group_n

def read_line(data, count):

    row = data.iloc[[count]]

    return row

def top_price_calculator(row, count):
    
    global top_price

    if (count)%level == 0: #top row of bid or ask
        if row.iloc[0, 2] == 0: #bid
            top_price[0] = row.iloc[0,0]
            History.iloc[0, 2] = top_price[0]
        
        if row.iloc[0, 2] == 1: #ask
            top_price[1] = row.iloc[0,0]
            History.iloc[0,3] = top_price[1]

def mean_price_calculator(row, count):
    
    global mean_price

    if (count)%(level*2) == 0: #resetting when new group starts
        mean_price = [0,0]

    if row.iloc[0,2] == 0: #bid
        mean_price[0] = mean_price[0] + row.iloc[0,0]
    
    if row.iloc[0,2] == 1: #ask
        mean_price[1] = mean_price[1] + row.iloc[0,0]
    
    if (count+1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mean_price[0] = mean_price[0] / level
        mean_price[1] = mean_price[1] / level

def mid_price_calculator(row, count):

    global mid_price

    if (count)%(level*2) == 0: #resetting when new group starts
        mid_price = [0,0]

    if level%2 == 1:
        if (count+1)%(level*2) == (level)/2: #mid of bid
            mid_price[0] = row.iloc[0,0]

        if (count+1)%(level*2) == (level+1)/2 + level: #mid of ask
            mid_price[1] = row.iloc[0,0]
    
    if level%2 == 0:
        if (count+1)%(level*2) == level/2 or (count+1)%(level*2) == level/2 + 1 :
            mid_price[0] = mid_price[0] + row.iloc[0,0]
        
        if (count+1)%(level*2) == level/2 + level or (count+1)%(level*2) == level/2 + 1 + level :
            mid_price[1] = mid_price[1] + row.iloc[0,0]

        if ((count+1) + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
            mid_price[0] = mid_price[0]/2
            mid_price[1] = mid_price[1]/2

def top_quantity_calculator(row, count):

    global top_quantity

    if (count)%level == 0: #top row of bid or ask
        if row.iloc[0, 2] == 0: #bid
            top_quantity[0] = row.iloc[0,1]
        
        if row.iloc[0, 2] == 1: #ask
            top_quantity[1] = row.iloc[0,1]

def mean_quantity_calculator(row, count):
    
    global mean_quantity

    if (count)%(level*2) == 0: #resetting when new group starts
        mean_quantity = [0,0]

    if row.iloc[0,2] == 0: #bid
        mean_quantity[0] = mean_quantity[0] + row.iloc[0,1]
    
    if row.iloc[0,2] == 1: #ask
        mean_quantity[1] = mean_quantity[1] + row.iloc[0,1]
    
    if (count+1)%(level*2) == 0 and count != 0: #at each point when a group ends
        #giving sum of bid & ask quantity for Book D
        History.iloc[0, 0] = mean_quantity[0]
        History.iloc[0, 1] = mean_quantity[1]

        #calculating each mean quantity
        mean_quantity[0] = mean_quantity[0] / level
        mean_quantity[1] = mean_quantity[1] / level

def mid_quantity_calculator(row, count):

    global mid_quantity

    if (count)%(level*2) == 0: #resetting when new group starts
        mid_quantity = [0,0]

    if level%2 == 1:
        if (count+1)%(level*2) == (level)/2: #mid of bid
            mid_quantity[0] = row.iloc[0,1]

        if (count+1)%(level*2) == (level+1)/2 + level: #mid of ask
            mid_quantity[1] = row.iloc[0,1]
    
    if level%2 == 0:

        if (count+1)%(level*2) == level/2 or (count+1)%(level*2) == level/2 + 1 :
            mid_quantity[0] = mid_quantity[0] + row.iloc[0,1]
        
        if (count+1)%(level*2) == level/2 + level or (count+1)%(level*2) == level/2 + 1 + level :
            mid_quantity[1] = mid_quantity[1] + row.iloc[0,1]

        if ((count+1) + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
            mid_quantity[0] = mid_quantity[0]/2
            mid_quantity[1] = mid_quantity[1]/2
    
def book_imbalance_calculator(row, count, ratio, interval):

    global askQty, bidQty, askPx, bidPx, top_price

    if (count)%(level*2) == 0: #resetting when new group starts
        askQty, bidQty, askPx, bidPx = 0, 0, 0, 0

    if row.iloc[0,2] == 0: #bid
        bidQty = bidQty + row.iloc[0,1] ** ratio
        bidPx = bidPx + row.iloc[0,0] * bidQty

    if row.iloc[0,2] == 1: #ask
        askQty = askQty + row.iloc[0,1] ** ratio
        askPx = askPx + row.iloc[0,0] * askQty
    
    if (count+1)%(level*2) == 0 and count != 0: #at each point when a group ends
        book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty))/(bidQty + askQty)
        book_imbalance = (book_price - (top_price[0] + top_price[1])/2)/interval

        if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
            features['book_imbalance_' + str(ratio) + '_' + str(interval) + 'sec'] = book_imbalance

def mid_price_top(count):
    
    if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mid_price_val = (top_price[0] + top_price[1])/2
        features['mid_price_top'] = mid_price_val

def mid_price_mean(count):
    
    if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mid_price_val = (mean_price[0] + mean_price[1])/2
        features['mid_price_mean'] = mid_price_val

def mid_price_mid(count):
    
    if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mid_price_val = (mid_price[0] + mid_price[1])/2
        features['mid_price_mid'] = mid_price_val

def mid_price_market_top(count):

    if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mid_price_val = (top_price[0] * top_quantity[1] + top_price[1] * top_quantity[0]) / (top_quantity[0] + top_quantity[1])
        features['mid_price_market_top'] = mid_price_val

def mid_price_market_mean(count):

    if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mid_price_val = (mean_price[0] * mean_quantity[1] + mean_price[1] * mean_quantity[0]) / (mean_quantity[0] + mean_quantity[1])
        features['mid_price_market_mean'] = mid_price_val

def mid_price_market_mid(count):

    if (count + 1)%(level*2) == 0 and count != 0: #at each point when a group ends
        mid_price_val = (mid_price[0] * mid_quantity[1] + mid_price[1] * mid_quantity[0]) / (mid_quantity[0] + mid_quantity[1])
        features['mid_price_market_mid'] = mid_price_val
         
def write_timestamp(row):
    features['timestamp'] = row.iloc[0, 3]
    History['timestamp'] = row.iloc[0, 3]

def main():

    global level
    global top_price, mean_price, mid_price
    global top_quantity, mean_quantity, mid_quantity

    global features, History

    History = pd.DataFrame({'bidQty':[0], 'askQty':[0], 'bidTop':[0], 'askTop':[0], 'timestamp':[0]})

    features = pd.DataFrame({}, index = ['0'])

    top_price, mean_price, mid_price = [0,0], [0,0], [0,0]
    top_quantity, mean_quantity, mid_quantity = [0,0], [0,0], [0,0]

    file = ['./raw/2023-05-25-Bithumb-BTC-orderbook.csv', './raw/2023-05-25-Bithumb-BTC-tradebook.csv']
    file[0] = input('type file name: ')
    try:
        headname = "-".join("".join(file[0].split('/')[2]).split('-')[0:5])
    except:
        print('ERROR!!!!')
        exit()
    
    orderbook, level, group_n = read_orderbook(file[0])

    for i in range(len(orderbook)):

        features = pd.DataFrame({}, index = ['0'])

        orderbook_row = read_line(orderbook, i)

        top_price_calculator(orderbook_row, i)
        mean_price_calculator(orderbook_row, i)
        mid_price_calculator(orderbook_row, i)

        top_quantity_calculator(orderbook_row, i)
        mean_quantity_calculator(orderbook_row, i)
        mid_quantity_calculator(orderbook_row, i)

        mid_price_top(i)
        mid_price_mean(i)
        mid_price_mid(i)

        mid_price_market_top(i)
        mid_price_market_mean(i)
        mid_price_market_mid(i)

        book_imbalance_calculator(orderbook_row, i, 0.5, 1)

        write_timestamp(orderbook_row)

        if (i + 1)%(level*2) == 0 and i != 0: #at each end of the time stamp group
            write_csv(features, headname+'-orderbook-features.csv')
            write_csv(History, headname+'-BQ-AQ-BT-AT.csv')

            for i in range(5): #reset History
                History.iloc[0, i] = 0


main()