#!/usr/bin/env python
import sys, re, json, pyspark_cassandra
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming

########################################################################################################################
# Utility Functions
########################################################################################################################
"""
This function parses a dictionary to grab two items - ZIP code and timestamp.
Timestamp is reduced to Day, Hour, Minute and Second only.

Input  : Dictionary
Output : List (ZIP code, int)
"""
def parse_dictionary_retrieve_date(me_dict):
    m = re.search('(\d\d\d\d\d\d(\d\d)-(\d\d\d\d\d\d))', me_dict['timestamp']) # grab day, hour, minute and second from timestamp
    # error in data
    o = re.search('{(\d+)}', me_dict['house']['zipcode'])
    if o:
        house_zipcode = o.group(1)
    else:
        house_zipcode = me_dict['house']['zipcode']

    return (house_zipcode, int(m.group(2) + "" + m.group(3)))

"""
This function takes three arguments and returns a list with three items in it.
"""
def parse_dictionary(zipcode, date, count):
    return (zipcode, date, count) # grab three arguments and return as a list

########################################################################################################################
# Main
########################################################################################################################
if __name__ == "__main__":

    # arguments
    args = sys.argv
    spark_ip = str(args[1])
    kafka_ip = str(args[2])

    # configure spark instance
    conf = (SparkConf().setMaster("spark://%s:7077"%spark_ip)\
            .setAppName("streaming_trending_zipcode_by_second")\
            .set("spark.executor.memory", "1g")\
            .set("spark.cores.max", "3"))
    sc = SparkContext(conf = conf)

    # stream every 5 seconds
    ssc = StreamingContext(sc, 5)

    data = KafkaUtils.createStream(ssc, "%s:2181"%kafka_ip, "pyspark-streaming-zipcodes", {"real_data_2":4})
    parsed = data.map(lambda (something, json_line): json.loads(json_line))
    counts = parsed.map(lambda message: (parse_dictionary_retrieve_date(message), 1)).reduceByKey(lambda x,y: x + y) # map/reduce by (ZIP code, datetime)
    mapped = counts.map(lambda line: parse_dictionary(line[0][0], line[0][1], line[1])) # return a list
    mapped.saveToCassandra("tristate", "trending_zipcode_by_seconds_streaming",) # save RDD to cassandra

    ssc.start() # start the process
    ssc.awaitTermination()