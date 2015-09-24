import sys, re, json, pyspark_cassandra
from datetime import datetime, timedelta

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#######################################################################################################
def parse_dictionary_retrieve_date(me_dict):
    tmp_date = me_dict['timestamp']
    m = re.search('((\d\d\d\d\d\d\d\d)-(\d\d)\d\d\d\d)', tmp_date)
    new_date = m.group(2) + "" + m.group(3)
    return (me_dict['house']['zipcode'], int(new_date))

#######################################################################################################
def parse_dictionary(zipcode, date, count):
    return (zipcode, date, count)

#######################################################################################################
if __name__ == "__main__":
    conf = (SparkConf().setMaster("spark://ip-172-31-23-187:7077").setAppName("streaming_trending_zipcode_by_hour").set("spark.executor.memory", "2g"))
    sc = SparkContext(conf = conf)
    
    # stream every 10 seconds
    ssc = StreamingContext(sc, 10)

    data = KafkaUtils.createStream(ssc, "ec2-54-209-211-14.compute-1.amazonaws.com:2181", "pyspark-streaming", {"real_data_2":4})
    parsed = data.map(lambda (something, json_line): json.loads(json_line))
    counts = parsed.map(lambda message: (parse_dictionary_retrieve_date(message), 1)).reduceByKey(lambda x,y: x + y)
    mapped = counts.map(lambda line: parse_dictionary(line[0][0], line[0][1], line[1]))
    mapped.saveToCassandra("tristate", "trending_zipcode_by_seconds_streaming", ttl=timedelta(hours=6),)
    
    ssc.start()
    ssc.awaitTermination()