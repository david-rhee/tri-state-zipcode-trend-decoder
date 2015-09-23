import sys, re
from datetime import datetime, timedelta

if __name__ == "__main__":

    # m = re.search('(\d\d\d\d)(\d\d)(\d\d)(\d\d)', str(last_entry[1]))
    # 
    # date = datetime.datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)),int(m.group(4)))
    # date += datetime.timedelta(hours=1)
    # date = date.strftime("%Y/%m/%d/%H")
    # print(date)

    time_field = (datetime.now() - timedelta(hours=3)).strftime("/%Y/%m/%d/%H")
    print time_field
    print datetime.now().strftime("/%Y/%m/%d/%H")


    
## bash
## read log to see which file
## process
## and if success, then copy old to new data
## and write log

## bash
## check the latest directory in new data
## process
## and if success, then copy old to new data


# stress test
# create skew - increase data (spike it)
# invalid data - corrupt data - negative zipcodes
# kill random nodes


# 2 DBs (batch - cassandra and streaming - hbase?)

# does companies have tech blogs?