import sys, re, json, pyspark_cassandra
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf

#######################################################################################################
def parse_json_per_zipcode_by_hour(tmp_string):
    message_dict = json.loads(tmp_string)
    tmp_date = message_dict['timestamp']
    m = re.search('((\d\d\d\d\d\d\d\d)-(\d\d)\d\d\d\d)', tmp_date)
    new_date = m.group(2) + "" + m.group(3)
    return (message_dict['house']['zipcode'], int(new_date))

#######################################################################################################
def parse_dictionary(zipcode, date, count):
    return (zipcode, date, count)

if __name__ == "__main__":
    conf = (SparkConf().setMaster("spark://ip-172-31-23-187:7077").setAppName("batch_trending_zipcode_by_hour").set("spark.executor.memory", "2g"))
    sc = SparkContext(conf = conf)
    
    # process data 2 hours apart
    process_time = (datetime.now() - timedelta(hours=2)).strftime("%Y/%m/%d/%H")
    data = sc.textFile('hdfs://ec2-54-209-211-14.compute-1.amazonaws.com:9000/camus/topics/real_data_2/hourly/%s/*.gz'%process_time)
    counts = data.map(lambda line: (parse_json_per_zipcode_by_hour(line), 1)).reduceByKey(lambda a, b: a + b)
    mapped = counts.map(lambda line: parse_dictionary(line[0][0], line[0][1], line[1]))
    mapped.saveToCassandra("tristate", "trending_zipcode_by_hour",)