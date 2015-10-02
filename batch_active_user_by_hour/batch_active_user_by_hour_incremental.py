#!/usr/bin/env python
import sys, re, json, pyspark_cassandra
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf

########################################################################################################################
# Utility Functions
########################################################################################################################
"""
This function parses a JSON formatted message to grab three items - User ZIP code, House ZIP code and timestamp.
Timestamp is reduced to Year, Month, Day and Hour only.

Input  : String in JSON format
Output : List ((User ZIP code, House ZIP code), int)
"""
def parse_json_per_zipcode_by_hour(tmp_string):
    message_dict = json.loads(tmp_string) # jsonify the message and transform it into dictionary
    m = re.search('((\d\d\d\d\d\d\d\d)-(\d\d)\d\d\d\d)', message_dict['timestamp']) # grab year, month, day and hour from timestamp
    # error in data
    o = re.search('{(\d+)}', message_dict['house']['zipcode'])
    if o:
        house_zipcode = o.group(1)
    else:
        house_zipcode = message_dict['house']['zipcode']

    return ((house_zipcode, message_dict['user']['zipcode']), int(m.group(2) + "" + m.group(3))) # return ZIP codes (User and House) and parsed datetime as a list

"""
This function takes four arguments and returns a list with four items in it.
"""
def parse_dictionary(user_zipcode, house_zipcode, date, count):
    return (user_zipcode, date, house_zipcode, count) # grab four arguments and return as a list

########################################################################################################################
# Main
########################################################################################################################
if __name__ == "__main__":

    # arguments
    args = sys.argv
    spark_ip = str(args[1])
    hdfs_ip = str(args[2])

    # configure spark instance
    conf = (SparkConf().setMaster("spark://%s:7077"%spark_ip)\
            .setAppName("batch_active_user_by_hour_incremental")\
            .set("spark.executor.memory", "6g")\
            .set("spark.cores.max", "24"))
    sc = SparkContext(conf = conf)

    # process all data
    data = sc.textFile('hdfs://%s:9000/camus/topics/real_data_2/hourly/*/*/*/*/*.gz'%hdfs_ip)
    counts = data.map(lambda line: (parse_json_per_zipcode_by_hour(line), 1)).reduceByKey(lambda a, b: a + b) # map/reduce by ((User ZIP code, House ZIP code), datetime)
    mapped = counts.map(lambda line: parse_dictionary(line[0][0][0], line[0][0][1], line[0][1], line[1])) # return a list
    mapped.saveToCassandra("tristate", "active_user_by_hour",) # save RDD to cassandra