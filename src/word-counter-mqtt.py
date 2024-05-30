from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .config('spark.jars.packages', 'org.apache.bahir:spark-streaming-mqtt_2.11:2.4.0')\
    .getOrCreate()

sc = spark.sparkContext

ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")


# broker URI
brokerUrl = "tcp://127.0.0.1:1883"
topic = "words"

from mqtt import MQTTUtils
mqttStream = MQTTUtils.createStream(ssc, brokerUrl, topic, username="user1", password="password")

words = mqttStream.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
