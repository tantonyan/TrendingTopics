from flask import jsonify 
from flask import request
from flask import render_template
from app import app
from cassandra.cluster import Cluster
from operator import itemgetter, attrgetter

cluster = Cluster(['cassandra.trendinghashtags.net'])
session = cluster.connect('trends')
topCount = 20

@app.route('/')
@app.route('/index')
@app.route('/live')
def index():
   return render_template("live.html")

@app.route('/daily')
def email():
 return render_template("daily.html")

@app.route("/daily", methods=['POST'])
def daily_post():
 country = request.form["country"] 
 city = request.form["city"] 
 date = request.form["date"]
 
 stmt_main = "SELECT * FROM "# and country=%s and city=%s"
 table = "world_day_trends"
 params = [int(date)]
 stmt_0 = " WHERE dayslot=%s"
 stmt_1 = ""
 fCount = 0
 if (len(country) > 0):
    stmt_1 = " and country=%s"
    table = "country_day_trends"
    params.append(country)
    fCount = 1
    if (len(city) > 0):
        stmt_1 += " and city=%s"
        table = "city_day_trends" 
	params.append(city)
        fCount = 2
 stmt = stmt_main + table + stmt_0 + stmt_1
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
 rCount = len(jsonresponse)
 sortedJson = sorted(list(jsonresponse), key=itemgetter('count'), reverse=True)[:topCount]
 return render_template("daily.html", output=sortedJson, fields=fCount, totalTrends=rCount)

@app.route('/api/topics-day/')
@app.route('/api/topics-day/<dayslot>/')
@app.route('/api/topics-day/<dayslot>/<country>/')
@app.route('/api/topics-day/<dayslot>/<country>/<city>')
def get_tweets_day(dayslot=None, country=None, city=None):
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = "" # will add constraints
	stmt_limit = " limit 1000"
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

        return jsonify(country_day_trends=jsonresponse)


@app.route('/api/topics-minute/')
@app.route('/api/topics-minute/<minuteslot>/')
@app.route('/api/topics-minute/<minuteslot>/<country>/')
@app.route('/api/topics-minute/<minuteslot>/<country>/<city>')
def get_tweets_minute(minuteslot=None, country=None, city=None):
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = "" # will add constraints
	stmt_limit = " limit 1000"
	params = []
	table = "world_minute_trends" # if minuteslot is not specified still use world
	# find and add the optional url parameters
        if (minuteslot is not None):
	    stmt_1 = " WHERE minuteslot=%s"
	    params.append(long(minuteslot))
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
            jsonresponse = [{"minuteslot": x.minuteslot, "country": x.country, "city": x.city, "topic": x.topic, "count": x.count} for x in response_list]
	elif (country is not None):
            jsonresponse = [{"minuteslot": x.minuteslot, "country": x.country, "topic": x.topic, "count": x.count} for x in response_list]
	else: 
            jsonresponse = [{"minuteslot": x.minuteslot, "topic": x.topic, "count": x.count} for x in response_list]

        return jsonify(country_day_trends=jsonresponse)


# locations -- /loc/[country]/[city]
@app.route('/api/loc/') # all locations
@app.route('/api/loc/<country>/') # only for a selected country
@app.route('/api/loc/<country>/<city>') # locations for a given city
def get_locations(country=None, city=None):
	stmt_main = "SELECT * FROM " # the main part of statements...
	stmt_1 = "" # will add constraints
	stmt_limit = " limit 1000"
	params = []
	table = "tweet_locations_country" # table name to be used - country for world and country views
	# find and add the optional url parameters
        if (country is not None):
	    stmt_1 = " WHERE country=%s"
	    params.append(country)
            if (city is not None): # will only happen if the country is set
		stmt_1 += " and city=%s"
		table = "tweet_locations_city"
	        params.append(city)
		
	stmt = stmt_main + table + stmt_1 + stmt_limit
        response = session.execute(stmt, parameters=params)
        response_list = []
        for val in response:
             response_list.append(val)

	if (city is not None):
	    jsonresponse = [{"country": x.country, "city": x.city, "latitude": x.lat, "longitude": x.long, "time_ms": x.time_ms} for x in response_list]
	else:
	    jsonresponse = [{"country": x.country, "latitude": x.lat, "longitude": x.long, "time_ms": x.time_ms} for x in response_list]

        return jsonify(tweet_locations=jsonresponse)

