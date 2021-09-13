#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import pymongo
import requests
import csv
import json

# In[23]:

def initialInvestment(investment):

    def getStockQuotes():
        stocks_quotes = {}
        markets = {'NASDAQ': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download',
            'NYSE': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download'}
        with requests.Session() as s:
            for market in markets:
                download = s.get(markets[market])
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)
                for row in my_list[1:]:
                    stock = market+': '+row[0].replace("^", "-")
                    stocks_quotes[stock] = row[2]
        return stocks_quotes


    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    mydb = myclient["stocksdb"]
    mycol = mydb["stockscol"]
    results = mycol.find()
    data = []
    for record in results:
        temp = []
        id = str(record['_id'])
        sentiment = str(record['sentiment_score'])
        temp.append(id)
        temp.append(sentiment)
        data.append(temp)
    df = pd.DataFrame(data, columns = ['Symbol', 'Sentiment_Score'])
    print(df.head())


    # In[30]:

    investment = 1000.0
    df = df.sort_values(by = 'Sentiment_Score', ascending = 0)
    quotes = getStockQuotes()
    def getquote(symbol):
        return quotes[symbol]
    df['Buy_Price'] = df['Symbol'].apply(getquote)

    # In[34]:


    df.head()


    # In[32]:


    df_portfolio = pd.DataFrame(columns = ['Stock', 'Buy_Price', 'Shares', 'Total'])


    # In[55]:


    df_portfolio.head()


    # In[69]:


    stk_name = []


    # In[70]:


    stocks = df['Symbol']


    # In[84]:


    stk_name = {}
    #n = len(prices)
    cp_inv = investment
    print(stocks)
    for stk in stocks:
        x = (df.loc[df['Symbol'] == stk, 'Buy_Price']).values
        price = float(x[0])
        y = (df.loc[df['Symbol'] == stk, 'Sentiment_Score']).values
        senti = y[0]
        if cp_inv == investment:
            if price * 3 > ((0.67) * cp_inv) or senti < 0:
                continue
            else:
                cp_inv = cp_inv - (price * 3)
                temp = price * 3
                print(temp)
                print(cp_inv)
                stk_name[stk] = [3, senti, price, temp]
        else:
            if cp_inv - (price * 3) > 0 and senti > 0:
                cp_inv = cp_inv - (price * 3)
                temp = price * 3
                print(temp)
                print(cp_inv)
                stk_name[stk] = [3, senti, price, temp]
    result = []
    for stck in stk_name.keys():
        num_shares = stk_name[stck]
        myuser = mydb["userportfolio"]
        myuser.insert_one(
            {"userid": "User001", "stock": stck, "numshares": stk_name[stck][0], "buyprice": stk_name[stck][2], "totalspent": stk_name[stck][3], "currsentiment": stk_name[stck][1]}
        )

        result.append({"stock": stck, "numshares": stk_name[stck][0], "buyprice": stk_name[stck][2], "totalspent": stk_name[stck][3], "currsentiment": stk_name[stck][1]})
    mysaving = mydb["usersavings"]
    mysaving.insert_one(
        {"_id": "User001", "savings": cp_inv}
    )
    final = {'pl': 0, 'dicte':result}
    return final