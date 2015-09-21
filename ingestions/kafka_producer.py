#!/usr/bin/env python
import os, sys, six, random, time

from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer

from kafka_producer_utils import parse_prefilter_data, build_distribution_based_zipcode_list, build_random_zipcode_list, emit_random_zipcode_price

class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_house_centric_msgs(self, source_symbol, topic):
        # Declare variables
        timing_list = []
    
        # Read throughput timing distribution
        with open('data/throughput/throughput_timing.txt', 'rU') as infile:
            for line in infile :
                timing_list.append(line.strip())
    
        # 0 to 86
        for x in range(0,86):        
            zipcode_sales_dict = parse_prefilter_data('data/tri_zipcode_sales/%s.txt'%x)
            zipcode_price_dict = parse_prefilter_data('data/tri_zipcode_price/%s.txt'%x)
        
            random_zipcode_sales_list = build_random_zipcode_list(zipcode_sales_dict)
            distribution_zipcode_sales_list = build_distribution_based_zipcode_list(zipcode_sales_dict)
    
            t_end = time.time() + 60 * 60 # every hour
    
            while time.time() < t_end:
                time_field = datetime.now().strftime("%Y%m%d-%H%M%S")
                user_id_field = 0
                random_number = random.randint(0, (len(random_zipcode_sales_list)-1)) 
                user_zipcode = random_zipcode_sales_list[random_number]
                house_field = emit_random_zipcode_price(distribution_zipcode_sales_list, zipcode_price_dict)
                str_fmt = """{{"timestamp":"{}","user":{{"id":"{}","zipcode":"{}"}},"house":{}}}"""
                message_info = str_fmt.format(time_field, user_id_field, user_zipcode, house_field)
        
                print message_info
                self.producer.send_messages(topic, source_symbol, message_info)
    
                if float(timing_list[x]) != 0:
                    time.sleep(float(timing_list[x]))

    def produce_user_centric_msgs(self, source_symbol, topic):
        user_id_list = ('1', '2')
        user_zipcode_list = ('10461', '07304')
        house_zipcode_list = ('10545', '07304')
        price_list = ('315789', '299679')

        for i in range(0, 2):    
            t_end = time.time() + 60 * 1440 # for 24 hours
    
            while time.time() < t_end:
                time_field = datetime.now().strftime("%Y%m%d-%H%M%S")
                user_id_field = user_id_list[i]
                user_zipcode = user_zipcode_list[i]
                another_random_number = random.randint(int(float(price_list[i])-(float(price_list[i])*0.3)), int(float(price_list[i])+(float(price_list[i])*0.5)))
                house_field = '{"zipcode":"{%s}","price":"{%s}"}'%(house_zipcode_list[i], another_random_number)
                str_fmt = """{{"timestamp":"{}","user":{{"id":"{}","zipcode":"{}"}},"house":{}}}"""
                message_info = str_fmt.format(time_field, user_id_field, user_zipcode, house_field)
        
                print message_info
                self.producer.send_messages(topic, source_symbol, message_info)
    
                time.sleep(600) # send message every 10 minutes

if __name__ == "__main__":

    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    topic = str(args[3])
    option = str(args[4])

    prod = Producer(ip_addr)
    if option == 'house':
        prod.produce_house_centric_msgs(partition_key, topic)
    elif option == 'user':
        prod.produce_user_centric_msgs(partition_key, topic)