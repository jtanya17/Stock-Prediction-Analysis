#!/usr/bin/env python
# coding: utf-8

# In[5]:

import requests
import csv
import pymongo


def buyandsell():
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

    quotes = getStockQuotes()

    def getquote(symbol):
        return quotes[symbol]

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    mydb = myclient["stocksdb"]
    mycol = mydb["stockscol"]
    myuser = mydb["userportfolio"]
    mysave = mydb["usersavings"]
    stocks_data = mycol.find()
    portfolio_data = myuser.find()
    savings_data = mysave.find()
    savings = 0
    for record in savings_data:
        savings += float(record['savings'])

    stocks = {}
    chng_flag = True
    for record in stocks_data:
        stocks[str(record['_id'])] = [record['sentiment_score'], getquote(str(record['_id']))]

    portfolio = {}
    for record in portfolio_data:
        if record['userid']=="User001":
            portfolio[record['stock']] = [record['currsentiment'], record['numshares'], record['buyprice']]
            if chng_flag:
                portfolio[record['stock']][0] = float(portfolio[record['stock']][0]) - 0.2
                chng_flag = False
            print(record['stock'])

    # In[39]:

    """
    #Initialise sentiment scores for stocks
    stk_len = len(stocks)
    sent_scores = []
    for i in range(stk_len):
        x = round(random.uniform(-1, 1), 1)
        sent_scores.append(x)
    """

    # In[40]:

    """
    #Initialise sentiment scores for portfolio
    port_len = len(portfolio)
    sent_port = []
    for i in range(port_len):
        x = round(random.uniform(-1, 1), 1)
        sent_port.append(x)
    """

    # In[46]:


    #Check if stocks need to be sold and sell it if needed; also update the buffer amount
    for i in portfolio.keys():
            if portfolio[i][0] < stocks[i][0]:
                print('inside')
                portfolio.pop(i)
                savings += float(getquote(i)) * float(portfolio[i][1])

    # Remove stocks which are already in portfolio
    for stock in portfolio.keys():
        if (stocks[stock] is not None):
            stocks.pop(stock)
    print(savings)
    #Code to check current market value of stock and add to total investment (global variable)
    for stock in portfolio.keys():
        savings += float(getquote(stock)) - float(portfolio[stock][2])
    print(savings)
    # In[47]:
    profit = round(savings, 2)


    #Rerun code for initial stock buying to use investment
    stk_name = {}
    #n = len(prices)
    cp_inv = savings
    #print(stocks.keys())

    for stk in stocks.keys():
        price = float(stocks[stk][1])
        senti = float(stocks[stk][0])
        if cp_inv == savings:
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
    myuser = mydb["userportfolio"]
    #myuser.delete_many({"userid" : "User001" })
    for stck in stk_name.keys():
        num_shares = stk_name[stck]
        myuser.insert_one(
            {"userid": "User001", "stock": stck, "numshares": stk_name[stck][0], "buyprice": stk_name[stck][2], "totalspent": stk_name[stck][3], "currsentiment": stk_name[stck][1]}
        )
    mysaving = mydb["usersavings"]
    mysaving.update_one(
        { "_id": "User001" },
          { "$set": { "savings": cp_inv } }
    )
    myuser = mydb["userportfolio"]
    portfolio_data = myuser.find()
    result = []
    for record in portfolio_data:
        result.append({"stock": record['stock'], "numshares": record['numshares'], "buyprice": record['buyprice'],
                       "totalspent": 0, "currsentiment": record['currsentiment']})
    final = {'pl': profit, 'dicte': result}
    return final

