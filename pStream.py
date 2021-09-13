from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import nltk
import warnings
warnings.filterwarnings('ignore')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext.getOrCreate(conf=conf)

    # Creating a streaming context with batch interval of 10 sec
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    stream(ssc, 300)


def stream(ssc, duration):
    delim = "$$$$"
    kstream = KafkaUtils.createDirectStream(ssc, topics=['newsstream'],
                                            kafkaParams={"metadata.broker.list": 'localhost:9092'})
    news = kstream.map(lambda x: x[1].encode("ascii", "ignore"))

    # Symbol    Stock   Date_Published  Current_Quote   Sentiment
    datardd = news.map(lambda x: x.split(delim)).map(lambda x: (x[0], x[1], x[2], x[3], sia.polarity_scores(x[4])['compound']))
    datardd.pprint()

    """
        Connect database and fetch stocks data of the current user
    """

    # Start the computation
    ssc.start()
    #ssc.awaitTerminationOrTimeout(duration)
    ssc.awaitTermination()
    ssc.stop(stopGraceFully=True)


if __name__ == "__main__":
    main()
