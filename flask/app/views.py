from flask import jsonify 
from flask import request
from flask import render_template
from app import app
from cassandra.cluster import Cluster
from operator import itemgetter, attrgetter
from datetime import date, datetime, timedelta
import collections
import time

#cluster = Cluster(['cassandra.trendinghashtags.net'])
#cluster = Cluster(['54.174.164.10', '54.175.246.246', '54.85.147.238'])
cluster = Cluster(['52.23.200.250', '54.175.185.121', '54.175.183.189'])
session = cluster.connect('trends')
topCount = 20

@app.route('/')
@app.route('/live')
def live():
 return render_template("live.html")

@app.route('/live', methods=['POST'])
@app.route("/", methods=['POST'])
def live_post():
 country = request.form["country"] 
 city = request.form["city"] 
 return render_template("live.html", country=country, city=city)


@app.route('/stats')
def stats():
   dayX = []
   dayS_t = {}

   response = session.execute("select * from world_day_counts")
   response_list = []
   for val in response:
     response_list.append(val)
   jsonresponse = [{"dayslot": x.dayslot, "type": x.count_type, "count": x.count} for x in response_list]
   jsonS = sorted(jsonresponse, key=itemgetter("dayslot"))
   for item in jsonS:
	dayS_t[item["type"]] = [] # assign arrays to all types
   for item in jsonS:
	sd = str(item["dayslot"])
	day = sd[:4] + "/" + sd[4:6] + "/" + sd[6:]
	if day not in dayX:
	    dayX.append(day)
	dayS_t[item["type"]].append(item["count"])
   series_d = []
   for name in dayS_t:
	series_d.append({"name": str(name), "data": dayS_t[name]})

   
   hourX = []
   hourS_t = {}
   response = session.execute("select * from world_hour_counts")
   response_list = []
   for val in response:
     response_list.append(val)
   jsonresponse = [{"hourslot": x.hourslot, "type": x.count_type, "count": x.count} for x in response_list]
   jsonS = sorted(jsonresponse, key=itemgetter("hourslot"))
   for item in jsonS:
	hourS_t[item["type"]] = [] # assign arrays to all types
   for item in jsonS:
	sd = str(item["hourslot"])
	hour = sd[:4] + "/" + sd[4:6] + "/" + sd[6:8] + " " + sd[8:10] + "h"
	if hour not in hourX:
	    hourX.append(hour)
	hourS_t[item["type"]].append(item["count"])
   series_h = []
   for name in hourS_t:
	series_h.append({"name": str(name), "data": hourS_t[name]})


#   response = session.execute("select * from world_hour_counts")
   
   return render_template("stats.html", series_d=series_d, xAxis_d=dayX, series_h=series_h, xAxis_h=hourX)

@app.route('/hourly')
def hourly():
 hourago = datetime.today() - timedelta(hours = 1)
 hourslot = hourago.strftime('%Y%m%d%H')
 stmt_main = "SELECT * FROM "# and country=%s and city=%s"
 table_top = "world_hour_top_trends"
 params = [int(hourslot)]
 stmt_0 = " WHERE hourslot=%s"
 stmt = stmt_main + table_top + stmt_0
 response = session.execute(stmt, parameters=params)
 response_list = []
 for val in response:
     response_list.append(val)
 
 jsonresponse = [{"hourslot": x.hourslot, "topic": x.topic, "count": x.count} for x in response_list]
 sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=True)[:topCount]
 counts_stmt = stmt_main + "world_hour_counts" + stmt_0
 response = session.execute(counts_stmt, parameters=params)
 counts = {}
 for val in response:
  counts[val.count_type] = int(val.count)
 if ('trends' in counts) and ('tagged' in counts) and ('total' in counts):
  return render_template("hourly.html", output=sortedJson, fields=0, totalTrends=counts["trends"], totalTagged=counts["tagged"], totalTweets=counts["total"])
 return render_template("hourly.html", output=sortedJson, fields=0)

@app.route("/hourly", methods=['POST'])
def hourly_post():
 country = request.form["country"] 
 city = request.form["city"] 
 hour = request.form["hour"]
 
 stmt_main = "SELECT * FROM "# and country=%s and city=%s"
 table = "world_hour_trends"
 table_top = "world_hour_top_trends"
 params = [int(hour)]
 stmt_0 = " WHERE hourslot=%s"
 stmt_1 = ""
 fCount = 0
 if (len(country) > 0):
    stmt_1 = " and country=%s"
    table = "country_hour_trends"
    table_top = "country_hour_top_trends"
    params.append(country)
    fCount = 1
    if (len(city) > 0):
        stmt_1 += " and city=%s"
        table = "city_hour_trends" 
        table_top = "city_hour_top_trends" 
	params.append(city)
        fCount = 2
 stmt = stmt_main + table_top + stmt_0 + stmt_1
 response = session.execute(stmt, parameters=params)
 response_list = []
 for val in response:
     response_list.append(val)
 if (fCount == 2):
     jsonresponse = [{"hourslot": x.hourslot, "country": x.country, "city": x.city, "topic": x.topic, "count": x.count} for x in response_list]
 elif (fCount == 1):
     jsonresponse = [{"hourslot": x.hourslot, "country": x.country, "topic": x.topic, "count": x.count} for x in response_list]
 else:
     jsonresponse = [{"hourslot": x.hourslot, "topic": x.topic, "count": x.count} for x in response_list]
 sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=True)[:topCount]
 stmt = "select count(*) from " + table + stmt_0 + stmt_1
 trendCount = session.execute(stmt, parameters=params)
 tCount = trendCount[0][0]
 return render_template("hourly.html", output=sortedJson, fields=fCount, totalTrends=tCount, city=city, country=country)

@app.route('/daily')
def daily():
 
 yesterday = date.today() - timedelta(1)
 dayslot = yesterday.strftime('%Y%m%d')
 stmt_main = "SELECT * FROM "# and country=%s and city=%s"
 table_top = "world_day_top_trends"
 params = [int(dayslot)]
 stmt_0 = " WHERE dayslot=%s"
 stmt = stmt_main + table_top + stmt_0
 response = session.execute(stmt, parameters=params)
 response_list = []
 for val in response:
     response_list.append(val)
 
 jsonresponse = [{"dayslot": x.dayslot, "topic": x.topic, "count": x.count} for x in response_list]
 sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=True)[:topCount]
 counts_stmt = stmt_main + "world_day_counts" + stmt_0
 response = session.execute(counts_stmt, parameters=params)
 counts = {}
 for val in response:
  counts[val.count_type] = int(val.count)
 if ('trends' in counts) and ('tagged' in counts) and ('total' in counts):
  return render_template("daily.html", output=sortedJson, fields=0, totalTrends=counts["trends"], totalTagged=counts["tagged"], totalTweets=counts["total"])
 return render_template("daily.html", output=sortedJson, fields=0)
# return render_template("daily.html")

@app.route("/daily", methods=['POST'])
def daily_post():
 country = request.form["country"] 
 city = request.form["city"] 
 date = request.form["date"]
 
 stmt_main = "SELECT * FROM "# and country=%s and city=%s"
 table = "world_day_trends"
 table_top = "world_day_top_trends"
 params = [int(date)]
 stmt_0 = " WHERE dayslot=%s"
 stmt_1 = ""
 fCount = 0
 if (len(country) > 0):
    stmt_1 = " and country=%s"
    table = "country_day_trends"
    table_top = "country_day_top_trends"
    params.append(country)
    fCount = 1
    if (len(city) > 0):
        stmt_1 += " and city=%s"
        table = "city_day_trends" 
        table_top = "city_day_top_trends" 
	params.append(city)
        fCount = 2
 stmt = stmt_main + table_top + stmt_0 + stmt_1
 response = session.execute(stmt, parameters=params)
 response_list = []
 for val in response:
     response_list.append(val)
 if (fCount == 2):
     jsonresponse = [{"dayslot": x.dayslot, "country": x.country, "city": x.city, "topic": x.topic, "count": x.count} for x in response_list]
 elif (fCount == 1):
     jsonresponse = [{"dayslot": x.dayslot, "country": x.country, "topic": x.topic, "count": x.count} for x in response_list]
 else:
     jsonresponse = [{"dayslot": x.dayslot, "topic": x.topic, "count": x.count} for x in response_list]
 sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=True)[:topCount]
 stmt = "select count(*) from " + table + stmt_0 + stmt_1
 trendCount = session.execute(stmt, parameters=params)
 tCount = trendCount[0][0]
 return render_template("daily.html", output=sortedJson, fields=fCount, totalTrends=tCount, city=city, country=country)

@app.route('/api/topics-day/')
@app.route('/api/topics-day/<dayslot>/')
@app.route('/api/topics-day/<dayslot>/<country>/')
@app.route('/api/topics-day/<dayslot>/<country>/<city>')
def get_tweets_day(dayslot=None, country=None, city=None):
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = "" # will add constraints
	stmt_limit = " limit 10"
	params = []
	table = "world_day_trends" # if dayslot is not specified still use world
	# find and add the optional url parameters
        if (dayslot is not None):
	    stmt_1 = " WHERE dayslot=%s"
	    params.append(int(dayslot))
            if (country is not None):
                stmt_1 += " and country=%s"
	        table = "country_day_trends"
	        params.append(country)
                if (city is not None): # will only happen if the country is set
		    stmt_1 += " and city=%s"
		    table = "city_day_trends"
	            params.append(city)

	stmt = stmt_main + table + stmt_1 + stmt_limit
        response = session.execute(stmt, params)
        response_list = []
        for val in response:
             response_list.append(val)

	if (city is not None):
            jsonresponse = [{"dayslot": x.dayslot, "country": x.country, "city": x.city, "topic": x.topic, "count": x.count} for x in response_list]
	elif (country is not None):
            jsonresponse = [{"dayslot": x.dayslot, "country": x.country, "topic": x.topic, "count": x.count} for x in response_list]
	else: 
            jsonresponse = [{"dayslot": x.dayslot, "topic": x.topic, "count": x.count} for x in response_list]

        return jsonify(country_day_trends=jsonresponse)


@app.route('/api/topics-hour/')
@app.route('/api/topics-hour/<hourslot>/')
@app.route('/api/topics-hour/<hourslot>/<country>/')
@app.route('/api/topics-hour/<hourslot>/<country>/<city>')
def get_tweets_hour(hourslot=None, country=None, city=None):
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = "" # will add constraints
	stmt_limit = " limit 1000"
	params = []
	table = "world_hour_trends" # if hourslot is not specified still use world
	# find and add the optional url parameters
        if (hourslot is not None):
	    stmt_1 = " WHERE hourslot=%s"
	    params.append(int(hourslot))
            if (country is not None):
                stmt_1 += " and country=%s"
	        table = "country_hour_trends"
	        params.append(country)
                if (city is not None): # will only happen if the country is set
		    stmt_1 += " and city=%s"
		    table = "city_hour_trends"
	            params.append(city)

	stmt = stmt_main + table + stmt_1 + stmt_limit
        response = session.execute(stmt, params)
        response_list = []
        for val in response:
             response_list.append(val)

	if (city is not None):
            jsonresponse = [{"hourslot": x.hourslot, "country": x.country, "city": x.city, "topic": x.topic, "count": x.count} for x in response_list]
	elif (country is not None):
            jsonresponse = [{"hourslot": x.hourslot, "country": x.country, "topic": x.topic, "count": x.count} for x in response_list]
	else: 
            jsonresponse = [{"hourslot": x.hourslot, "topic": x.topic, "count": x.count} for x in response_list]

        return jsonify(hour_trends=jsonresponse)


@app.route('/api/topics-minute/')
@app.route('/api/topics-minute/<country>/')
@app.route('/api/topics-minute/<country>/<city>/')
#@app.route('/api/topics-minute/<minuteslot>/')
#@app.route('/api/topics-minute/<minuteslot>/<country>/')
#@app.route('/api/topics-minute/<minuteslot>/<country>/<city>/')
def get_tweets_minute(country=None, city=None):
#        return jsonify(minute_trends="")
	minuteslot = long(time.strftime('%Y%m%d%H%M',time.localtime(time.time() - 120)))
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = "" # will add constraints
#	stmt_limit = " limit 1000"
	stmt_limit = ""
	params = []
	table = "world_minute_trends" # if minuteslot is not specified still use world
	# find and add the optional url parameters
        if (minuteslot is not None):
	    stmt_1 = " WHERE minuteslot=%s"
	    params.append(long(minuteslot))
            if (country is not None):
                stmt_1 += " and country=%s"
	        table = "country_minute_trends"
	        params.append(country)
                if (city is not None): # will only happen if the country is set
		    stmt_1 += " and city=%s"
		    table = "city_minute_trends"
	            params.append(city)

	stmt = stmt_main + table + stmt_1 + stmt_limit
#	return ("stmt: " + stmt)
        response = session.execute(stmt, params)
        response_list = []
        for val in response:
             response_list.append(val)

	if (city is not None):
            jsonresponse = [{"minuteslot": x.minuteslot, "country": x.country, "city": x.city, "topic": x.topic, "count": x.count} for x in response_list]
	elif (country is not None):
            jsonresponse = [{"minuteslot": x.minuteslot, "country": x.country, "topic": x.topic, "count": x.count} for x in response_list]
	else: 
            jsonresponse = [{"minuteslot": x.minuteslot, "topic": x.topic, "count": x.count} for x in response_list]

        sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=True)[:topCount]

        return jsonify(minute_trends=sortedJson)



@app.route('/api/rt/') # real time tweets 
@app.route('/api/rt/<topic>/<country>') # for a given country
@app.route('/api/rt/<topic>/<country>/<city>') # for a given city
#@app.route('/api/rt/<secslot>') # real time tweets 
#@app.route('/api/rt/<secslot>/<topic>/<country>') # for a given country
#@app.route('/api/rt/<secslot>/<topic>/<country>/<city>') # for a given city
def get_rt(topic=None, country=None, city=None):
#	return jsonify(tweets="")
        # we will fix the secslot here
	secslot = long(time.time()) - 60 # one minute delay for time diffs 
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = " WHERE secslot=%s" # will add more constraints
	params = [long(secslot)]
	stmt_limit = " limit 10"
	table = "rt_tweet_world" # table name to be used
        if (country is not None):
	    stmt_1 = " and topic=%s and country=%s"
	    table = "rt_tweet_country" # use country if specified
	    params.append(topic, country)
            if (city is not None): # will only happen if the country is set
		stmt_1 += " and city=%s"
		table = "rt_tweet_city"
	        params.append(city)
	stmt = stmt_main + table + stmt_1 + stmt_limit
        response = session.execute(stmt, parameters=params, timeout=0.8)
        response_list = []
        for val in response:
             response_list.append(val)

	if (city is not None):
	    jsonresponse = [{"country": x.country, "city": x.city, "topic": x.topic, "user": x.user, "time_ms": x.time_ms, "tweet": x.tweet} for x in response_list]
	elif (country is not None):
	    jsonresponse = [{"country": x.country, "topic": x.topic, "user": x.user, "time_ms": x.time_ms, "tweet": x.tweet} for x in response_list]
	else:
	    jsonresponse = [{"topic": x.topic, "user": x.user, "time_ms": x.time_ms, "tweet": x.tweet} for x in response_list]

        return jsonify(tweets=jsonresponse)
	

# locations -- /loc/[country]/[city]
@app.route('/api/loc/') # all locations
@app.route('/api/loc/<country>/') # only for a selected country
@app.route('/api/loc/<country>/<city>') # locations for a given city
def get_locations(country=None, city=None):
#	return jsonify(tweet_locations="")
	secslot = long(time.time()) - 60 # one minute delay for time diffs 
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = " WHERE secslot=%s" # will add more constraints
#	stmt_limit = " limit 20"
	params = [secslot]
	table = "rt_tweet_locations_world" # table name to be used
	# find and add the optional url parameters
        if (country is not None):
	    stmt_1 = " WHERE country=%s"
	    table = "rt_tweet_locations_country" # use country 
	    params.append(country)
            if (city is not None): # will only happen if the country is set
		stmt_1 += " and city=%s"
		table = "rt_tweet_locations_city"
	        params.append(city)
		
	stmt = stmt_main + table + stmt_1# + stmt_limit
        response = session.execute(stmt, parameters=params)
        response_list = []
        for val in response:
             response_list.append(val)

	if (city is not None):
	    jsonresponse = [{"country": x.country, "city": x.city, "latitude": x.lat, "longitude": x.long, "time_ms": x.time_ms} for x in response_list]
	elif (country is not None):
	    jsonresponse = [{"country": x.country, "latitude": x.lat, "longitude": x.long, "time_ms": x.time_ms} for x in response_list]
	else:
	    jsonresponse = [{"latitude": x.lat, "longitude": x.long, "time_ms": x.time_ms} for x in response_list]

#        sortedJson = sorted(list(jsonresponse), key=itemgetter('time_ms'), reverse=True)[:topCount*2]

        return jsonify(tweet_locations=jsonresponse)

@app.route('/api/timeslots/<slotname>') # hourly is optional
def get_timeslots(slotname):
  table = "world_day_top_trends" # default to day
  keyName = "dayslot"
  if (slotname == 'hour'):
    table = "world_hour_top_trends"
    keyName = "hourslot"
  elif (slotname == 'minute'):
    table = "world_minute_top_trends"
    keyName = "minuteslot"
  stmt = "SELECT DISTINCT " + keyName + " from " + table
  response = session.execute(stmt)
  response_list = []
  for val in response:
     response_list.append(val)
  jsonresponse = [x[0] for x in response_list]
  times = {}
  for item in jsonresponse:
    v = str(item)
    if (keyName == "dayslot"):
      times[item]  = v[:4] + "/" + v[4:6] + "/" + v[6:8]
    elif (keyName == "hourslot"):
      times[item]  = v[:4] + "/" + v[4:6] + "/" + v[6:8] + " " + v[8:] + "h"
    elif (keyName == "minuteslot"):
      times[item]  = v[:4] + "/" + v[4:6] + "/" + v[6:8] + " " + v[8:10] + ":" + v[10:] 
  sortedJson = collections.OrderedDict(sorted(times.items()))
  return jsonify(timeslots=sortedJson)

