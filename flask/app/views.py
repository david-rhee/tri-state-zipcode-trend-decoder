import re, time
from collections import OrderedDict
from datetime import datetime, timedelta
from flask import render_template, jsonify, request
from app import app
from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(['54.84.215.229'])
session = cluster.connect('tristate')
session.default_timeout = 30 # 30 seconds

######################################################################################################################################################################
# Utils
######################################################################################################################################################################
# Adjust to 5 decimals
def leadingzero(x) :
    return str(x).rjust(5, '0')

# return list of zipcodes
def return_zipcodes() :
    # Declare variables
    response_list = []

    # Read line by line
    #with open('/home/ubuntu/tri-state-zipcode-trend-decoder/flask/app/static/zipcodes.txt', 'rU') as infile:
    with open('/Users/drhee1/Documents/Codes/InsightDE/Codes/tri-state-zipcode-trend-decoder/flask/app/static/zipcodes.txt', 'rU') as infile:
        for line in infile :
            response_list.append(line.strip()) # strip

    return response_list

# return formatted date
def return_date(x) :
    m = re.search('(\d\d)(\d\d)(\d\d)(\d\d)', str(x))
    new_string = str(m.group(2) + ":" + m.group(3) + ":" + m.group(4))
    return new_string

######################################################################################################################################################################
# INDEX
######################################################################################################################################################################
###################################################################################
# index
@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

######################################################################################################################################################################
# REAL-TIME
######################################################################################################################################################################
###################################################################################
# last 5 seconds
@app.route('/realtime_zipcode')
def get_realtime_zipcode_last_5_seconds():

    search_time = (datetime.now() - timedelta(seconds=5)).strftime("%d%H%M%S")
    search_time = int(search_time)
    
    stmt = "SELECT house_zipcode, date, count FROM trending_zipcode_by_seconds_streaming where date >= %s limit 20 allow filtering"
    response = session.execute(stmt, parameters=[search_time])

    response_list = []
    for val in response:
        response_list.append(val)

    jsonresponse = [{"zipcode": str(x.house_zipcode), "date": return_date(x.date), "count": str(x.count)} for x in response_list]
    
    return jsonify(jsonresponse=jsonresponse)


######################################################################################################################################################################
# BATCH
######################################################################################################################################################################
###################################################################################
# zip code history - hourly
@app.route('/history_zipcode_hourly')
def history_zipcode_hourly_search():
    # Declare variables
    response_list = return_zipcodes()

    return render_template("history_zipcode_hourly_search.html", response_list=response_list)

@app.route("/history_zipcode_hourly", methods=['POST'])
def history_zipcode_hourly_post():
    zipcode = request.form["zipcode"]

    stmt = "SELECT house_zipcode, date, count FROM trending_zipcode_by_hour WHERE house_zipcode = %s ALLOW FILTERING"
    response = session.execute(stmt, parameters=[zipcode])
    
    tmp_dict = {}
    for val in response:
        date_string = str(val.date)
        date_time = datetime(year=int(date_string[0:4]), month=int(date_string[4:6]), day=int(date_string[6:8]), hour=int(date_string[9:11]))
        epoch = int(date_time.strftime("%s")) * 1000
        tmp_dict[epoch] = val.count

    keys = tmp_dict.keys()
    keys.sort()
    
    jsonresponse = []
    for key in keys:
        jsonresponse.append([key, tmp_dict[key]])

    return render_template("history_zipcode_hourly.html", zipcode=zipcode, jsonresponse=jsonresponse)

###################################################################################
# zip code history - daily
@app.route('/history_zipcode_daily')
def history_zipcode_daily_search():
    # Declare variables
    response_list = return_zipcodes()

    return render_template("history_zipcode_daily_search.html", response_list=response_list)

@app.route("/history_zipcode_daily", methods=['POST'])
def history_zipcode_daily_post():
    zipcode = request.form["zipcode"]

    stmt = "SELECT house_zipcode, date, count FROM trending_zipcode_by_day WHERE house_zipcode = %s ALLOW FILTERING"
    response = session.execute(stmt, parameters=[zipcode])
    
    tmp_dict = {}
    for val in response:
        date_string = str(val.date)
        date_time = datetime(year=int(date_string[0:4]), month=int(date_string[4:6]), day=int(date_string[6:8]))
        epoch = int(date_time.strftime("%s")) * 1000
        tmp_dict[epoch] = val.count

    keys = tmp_dict.keys()
    keys.sort()
    
    jsonresponse = []
    for key in keys:
        jsonresponse.append([key, tmp_dict[key]])

    return render_template("history_zipcode_daily.html", zipcode=zipcode, jsonresponse=jsonresponse)

@app.route("/history_zipcode_daily/api/<zipcode>")
def history_zipcode_daily_api(zipcode):
    stmt = "SELECT house_zipcode, date, count FROM trending_zipcode_by_day WHERE house_zipcode = %s ALLOW FILTERING"
    response = session.execute(stmt, parameters=[zipcode])

    response_list = []
    for val in response:
        response_list.append(val)

    jsonresponse = [{"zipcode": str(x.house_zipcode), "date": str(x.date), "count": str(x.count)} for x in response_list]

    return jsonify(jsonresponse=jsonresponse)

###################################################################################
# popular zip codes - hourly
@app.route('/popular_zipcode_hourly')
def batch_zipcode_hourly_search():
    time_now_minus_two = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
    return render_template("popular_zipcode_hourly_search.html", time_now_minus_two=time_now_minus_two)

@app.route("/popular_zipcode_hourly", methods=['POST'])
def batch_zipcode_hourly_post():
    search_date = request.form["search_date"]
    time_range = '[' + search_date + ']'

    state = request.form["state"]

    m = re.search('(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):\d\d:\d\d', search_date)
    search_date = m.group(1) + "" + m.group(2) + "" + m.group(3) + "" + m.group(4)

    stmt = "SELECT house_zipcode, count FROM trending_zipcode_by_hour WHERE date = %s ALLOW FILTERING"
    response = session.execute(stmt, parameters=[int(search_date)])

    jsonresponse = []
    for val in response:
        jsonresponse.append({"house_zipcode": leadingzero(val.house_zipcode), "count": val.count})

    if state == 'CT':
        return render_template("trending_ct.html", jsonresponse=jsonresponse)
    elif state == 'NJ':
        return render_template("trending_nj.html", jsonresponse=jsonresponse)
    elif state == 'NY':
        return render_template("trending_ny.html", jsonresponse=jsonresponse)

###################################################################################
# popular zip codes - daily
@app.route('/popular_zipcode_daily')
def batch_zipcode_daily_search():
    time_now_minus_two = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return render_template("popular_zipcode_daily_search.html", time_now_minus_two=time_now_minus_two)

@app.route("/popular_zipcode_daily", methods=['POST'])
def batch_zipcode_daily_post():
    search_date = request.form["search_date"]
    time_range = '[' + search_date + ']'

    state = request.form["state"]

    m = re.search('(\d\d\d\d)-(\d\d)-(\d\d)', search_date)
    search_date = m.group(1) + "" + m.group(2) + "" + m.group(3)

    stmt = "SELECT house_zipcode, count FROM trending_zipcode_by_day WHERE date = %s ALLOW FILTERING"
    response = session.execute(stmt, parameters=[int(search_date)])
    
    jsonresponse = []
    for val in response:
        jsonresponse.append({"house_zipcode": leadingzero(val.house_zipcode), "count": val.count})

    if state == 'CT':
        return render_template("trending_ct.html", jsonresponse=jsonresponse)
    elif state == 'NJ':
        return render_template("trending_nj.html", jsonresponse=jsonresponse)
    elif state == 'NY':
        return render_template("trending_ny.html", jsonresponse=jsonresponse)

###################################################################################
# active user - hourly
@app.route('/active_user_hourly')
def batch_active_user_hourly_search():
    # Declare variables
    response_list = return_zipcodes()
    time_now_minus_two = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")

    return render_template("popular_active_user_hourly_search.html", response_list=response_list, time_now_minus_two=time_now_minus_two)

@app.route("/active_user_hourly", methods=['POST'])
def batch_active_user_hourly_post():
    search_date = request.form["search_date"]
    time_range = '[' + search_date + ']'

    state = request.form["state"]
    zipcode = request.form["zipcode"]

    m = re.search('(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):\d\d:\d\d', search_date)
    search_date = m.group(1) + "" + m.group(2) + "" + m.group(3) + "" + m.group(4)

    stmt = "SELECT user_zipcode, count FROM active_user_by_hour WHERE date = %s and house_zipcode = %s ALLOW FILTERING"
        
    response = session.execute(stmt, parameters=[int(search_date), zipcode])

    jsonresponse = []
    for val in response:
        jsonresponse.append({"house_zipcode": leadingzero(val.user_zipcode), "count": val.count})

    if state == 'CT':
        return render_template("trending_ct.html", jsonresponse=jsonresponse)
    elif state == 'NJ':
        return render_template("trending_nj.html", jsonresponse=jsonresponse)
    elif state == 'NY':
        return render_template("trending_ny.html", jsonresponse=jsonresponse)

###################################################################################
# active user - daily
@app.route('/active_user_daily')
def batch_active_user_daily_search():
    # Declare variables
    response_list = return_zipcodes()
    time_now_minus_two = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    return render_template("popular_active_user_daily_search.html", response_list=response_list, time_now_minus_two=time_now_minus_two)

@app.route("/active_user_daily", methods=['POST'])
def batch_active_user_daily_post():
    search_date = request.form["search_date"]
    time_range = '[' + search_date + ']'

    state = request.form["state"]
    zipcode = request.form["zipcode"]

    m = re.search('(\d\d\d\d)-(\d\d)-(\d\d)', search_date)
    search_date = m.group(1) + "" + m.group(2) + "" + m.group(3)

    stmt = "SELECT user_zipcode, count FROM active_user_by_day WHERE date = %s and house_zipcode = %s ALLOW FILTERING"
        
    response = session.execute(stmt, parameters=[int(search_date), zipcode])

    jsonresponse = []
    for val in response:
        jsonresponse.append({"house_zipcode": leadingzero(val.user_zipcode), "count": val.count})

    if state == 'CT':
        return render_template("trending_ct.html", jsonresponse=jsonresponse)
    elif state == 'NJ':
        return render_template("trending_nj.html", jsonresponse=jsonresponse)
    elif state == 'NY':
        return render_template("trending_ny.html", jsonresponse=jsonresponse)
