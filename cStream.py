from kafka import SimpleProducer, KafkaClient
import requests
from dateutil.parser import parse
from datetime import datetime, timedelta
import time
import csv
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

def main():
    time.sleep(20)
    makeStream()
    scheduler = BlockingScheduler()
    scheduler.add_job(makeStream, 'interval', hours=1)
    scheduler.start()


def makeStream():
    client = KafkaClient("localhost:9092")
    producer = SimpleProducer(client)

    # Dow Jones Industrial Average 30 stocks
    dow30 = ['MMM', 'AXP', 'AAPL', 'BA', 'CAT', 'CVX', 'CSCO', 'KO', 'DIS', 'DOW', 'XOM', 'GS', 'HD', 'IBM', 'INTC', 'JNJ',
             'JPM', 'MCD', 'MRK', 'MSFT', 'NKE', 'PFE', 'PG', 'TRV', 'UTX', 'UNH', 'VZ', 'V', 'WMT', 'WBA']

    CSV_URL1 = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download'

    CSV_URL2 = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download'

    stock_quotes = {}
    with requests.Session() as s:
        download = s.get(CSV_URL1)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list1 = list(cr)

        download = s.get(CSV_URL2)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list2 = list(cr)
        my_list = my_list1+my_list2[1:]
        for row in my_list[1:]:
            if row[0] in dow30:
                stock_quotes[row[0]] = [row[1].replace("&#39;s", "\'"), row[2]]

    delim = '$$$$'

    for symbol in stock_quotes.keys():
        #f_dt = str((datetime.now() - timedelta(hours=1)).replace(microsecond=0).isoformat())
        f_dt = str((datetime.now() - timedelta(days=5)).replace(microsecond=0).isoformat())
        stock = stock_quotes[symbol][0]
        quote = stock_quotes[symbol][1]
        print('Stock being analyzed : ' + stock)
        # stock = stock.replace(" ", "%20")
        # stock = stock.replace("\'", "%2527")
        t_dt = str(datetime.now().replace(microsecond=0).isoformat())
        url = 'https://newsapi.org/v2/everything?q=' + stock + '&from='+f_dt+'&to='+t_dt+'&sortBy=publishedAt&apiKey=9714e1d74fb64495aaafdb54d4cdd0bc'
        response = requests.get(url)
        json_res = response.json()

        for post in json_res["articles"]:
            date_time = post["publishedAt"]
            #if parse(date_time).date() == last_hour.date() and parse(date_time).time() > last_hour.time():
            data = symbol + delim + stock + delim + quote + delim + date_time
            if post["description"] is not None:
                data += delim + post["description"]
            else:
                continue
            if post["content"] is not None:
                data += " " + post["content"]
            msg = data.encode('utf-8')
            producer.send_messages(b'newsstream', msg)


if __name__ == '__main__':
    main()