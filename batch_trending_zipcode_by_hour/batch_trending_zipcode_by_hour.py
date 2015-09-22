from __future__ import print_function

import sys, re, json

from pyspark import SparkConf, SparkContext
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

class BatchProcessor(object):

    def __init__(self, addr):
        conf = (SparkConf().setMaster("spark://ip-172-31-23-187:7077").setAppName("batch_trending_zipcode_by_hour").set("spark.executor.memory", "2g"))
        self.sc = SparkContext(conf = conf)

        cluster = Cluster(['172.31.23.187'])
        self.session = session = cluster.connect()
        self.session.set_keyspace("tristate")

    def parse_json_per_zipcode_by_hour(self, tmp_string):
        message_dict = json.loads(tmp_string)
        tmp_date = message_dict['timestamp']
        m = re.search('((\d\d\d\d\d\d\d\d)-(\d\d)\d\d\d\d)', tmp_date)
        new_date = m.group(2) + "" + m.group(3)
        return (message_dict['house']['zipcode'], int(new_date))

    def read_hdfs_file(self):
        return sc.textFile("hdfs://ec2-54-209-211-14.compute-1.amazonaws.com:9000/camus/topics/real_data_1/hourly/2015/09/17/12")
        
        #counts.saveToCassandra("playground", "by_zipcode_minute")

    def load_cassandra(self):
        query = SimpleStatement("""INSERT INTO by_zipcode_hour (date, house_zipcode, count)VALUES (%(a)s, %(b)s, %(c)s)""", consistency_level=ConsistencyLevel.ONE)
        for val in res:
            session.execute(query, dict(a=val[0][0], b=val[0][1], c=(val[1])))

    def main(self):
        infile = self.read_hdfs_file()
        counts = file.map(lambda line: (json_split(line), 1)).reduceByKey(lambda a, b: a + b)

#/usr/local/spark/bin/spark-submit --jars httpclient-4.2.5.jar --packages TargetHolding/pyspark-cassandra:0.1.5 --conf spark.cassandra.connection.host=ec2-54-209-211-14.compute-1.amazonaws.com minute_house_batch.py

if __name__ == "__main__":
    processor = BatchProcessor()
    processor.main()