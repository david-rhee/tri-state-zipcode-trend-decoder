#!/usr/bin/env python
import os, string, random, time
from datetime import datetime

''' 

PURPOSE: Utility functions for simulating user activities.
DATE:
Version:
AUTHOR: David Rhee
Modifier:

Parses the Home Sales list from http://www.zillow.com/research/data/ for the zipcode and number of houses sold
per month. The number of houses sold per month is used to increase the likelihood of zipcode with a higher houses
sold to be chosen during random sampling.

'''

########################################################################################################################
########################################################################################################################
# Pre-filetering Functions
######################################################################################################################## 
def prefilter_sales_zip_tri():
    """
    Pre-filter Sales_Zip_Tri.txt
    """
    # Declare variables
    line_count = 0
    sales_zip_dict = {}
    
    # Read line by line
    with open('data/original/Sales_Zip_Tri.txt', 'rU') as infile:
        for line in infile :
            if not line_count == 0 : # skip first line
                tmp_list = line.strip().split("\t") # split line into a list
                key = tmp_list[0]
                del tmp_list[0:2]
                sales_zip_dict[key] = tmp_list # store list into a dictionary
    
            line_count += 1
    
    keys = sales_dict.keys()
    keys.sort()
    
    #86 columns
    for x in range(0, 86):
        output_filename = 'data/tri_zipcode_sales/%s.txt'%x
    
        outfile = open(output_filename,'w')
        for key in keys:    
            outfile.write("%s\t%s\n"%(key,str(int(sales_dict[key][x])+1)))
        outfile.close()

def prefilter_zhvi_all_homes_zip():
    """
    Pre-filter Zip_Zhvi_AllHomes_Tri.txt
    """

    # Declare variables
    line_count = 0
    zhvi_all_homes_zip_dict = {}

    # Read line by line
    with open('data/original/Zip_Zhvi_AllHomes_Tri.txt', 'rU') as infile:
        for line in infile :
            if not line_count == 0 : # skip first line
                tmp_list = line.strip().split("\t") # split line into a list
                key = tmp_list[0]
                del tmp_list[0:4]
                zhvi_all_homes_zip_dict[key] = tmp_list # store list into a dictionary

            line_count += 1

    keys = zhvi_all_homes_zip_dict.keys()
    keys.sort()
    
    #86 columns
    for x in range(0, 86):
        output_filename = 'data/tri_zipcode_price/%s.txt'%x
    
        outfile = open(output_filename,'w')
        for key in keys:    
            outfile.write("%s\t%s\n"%(key,zhvi_all_homes_zip_dict[key][x]))
        outfile.close()

########################################################################################################################
# Utility Functions
########################################################################################################################
def parse_prefilter_data(filename):
    """
    Parses pre-filtered *.txt files
    """

    # Declare variables
    tmp_dict = {}

    # Read line by line
    with open(filename, 'rU') as infile:
        for line in infile :
            tmp_list = line.strip().split("\t") # split line into a list
            tmp_dict[tmp_list[0]] = tmp_list[1]

    return tmp_dict

def build_distribution_based_zipcode_list(sales_zip_dict):
    """
    This function builds a list based on historical data - it will be used as a distribution for randomly selecting zipcodes
    """

    # Declare variables
    distribution_zipcode_list = []
    
    keys = sales_zip_dict.keys()
    keys.sort()

    for key in keys:
        for x in range(0, int(sales_zip_dict[key])):
            distribution_zipcode_list.append(key)

    return distribution_zipcode_list

def build_random_zipcode_list(sales_zip_dict):
    """
    Builds a random zipcode list
    """

    # Declare variables
    random_zipcode_list = []
    
    keys = sales_zip_dict.keys()
    keys.sort()

    for key in keys:
        random_zipcode_list.append(key)

    return random_zipcode_list

def emit_random_zipcode_price(built_zipcode_list, zhvi_all_homes_zip_dict):
    """
    This function emits randomly selected zipcode and price message
    """

    random_number = random.randint(0, (len(built_zipcode_list)-1)) 
    zipcode = built_zipcode_list[random_number]
    tmp_price = zhvi_all_homes_zip_dict[str(zipcode)]

    another_random_number = random.randint(int(float(tmp_price)-(float(tmp_price)*0.3)), int(float(tmp_price)+(float(tmp_price)*0.5)))

    str_fmt = """{{"zipcode":"{}","price":"{}"}}"""
    message_info = str_fmt.format(zipcode, another_random_number)

    return message_info