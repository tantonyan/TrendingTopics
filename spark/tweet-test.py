from pyspark import SparkContext, SparkConf
import json
import sys
import datetime

import pyspark_cassandra


hdfs = "hdfs://ec2-54-209-187-157.compute-1.amazonaws.com:9000"
path = "/user/test-tweets/tweets-17-22.json"
keyspace = "trends"
topCount = 20 # to be used when inserting into top tables -- will spead up the reads later

#path = "/user/test-tweets/json-tweets-10.txt"
# can take the path to a folder/file as an argument to be added to the hdfs path
if len(sys.argv) > 1:
    path = sys.argv[1]

folder_in = hdfs + path

# take a datetime object and return a string for the minute slot
def convert_to_1m(dt):
    return int(dt.strftime('%Y%m%d%H%M'))

# take a datetime object and return a string for the hour slot
def convert_to_1h(dt):
    return int(dt.strftime('%Y%m%d%H'))

# take a datetime object and return a string for the day slot
def convert_to_1d(dt):
    return int(dt.strftime('%Y%m%d'))

# get poster's user
def userId(item):
    if ('user' in item) and isinstance(item['user'], dict):
	if 'id_str' in item['user']:
	    return item['user']['id_str']
    return None

# get a combined list of the hashtags and user mentions 
def trendItems(item):
    tags = []
    if ('entities' in item) and isinstance(item['entities'], dict):
	# only continue if we have a list (could be 'None')
	if 'hashtags' in item['entities']:
	    tags.extend(item['entities']['hashtags'])

	if 'user_mentions' in item['entities']:
	    tags.extend(item['entities']['user_mentions'])
    return tags

def tweetCountry(item):
    if ('place' in item) and isinstance(item['place'], dict):
        if 'country_code' in item['place']:
	    return item['place']['country_code']
    return None

def tweetCity(item):
    if ('place' in item) and isinstance(item['place'], dict):
        if 'name' in item['place']:
	    return item['place']['name']
    return None

# parse the next raw text line into a tweet -- just the needed elements
def tweet_from_json_line(json_line):
    try:
        item = json.loads(json_line)
    except:
	return None # not a json line -- just retun None

    if ('id_str' not in item) or ('timestamp_ms' not in item) or('text' not in item):
	return None # not what we expected

    tweet = {}
    tweet['t_id'] = item['id_str']
    tweet['u_id'] = userId(item) # just the userId is enough
    tweet['coords'] = item['coordinates'] # list [longitude,latitude]
    tweet['tags'] = trendItems(item) # will have the combined list of hashtags and user mentions
    tweet['city'] = tweetCity(item)
    tweet['country'] = tweetCountry(item) # the country code: US, UK, etc.

    # if any of the fields are missing return None as well
    if ((tweet['u_id'] is None) or (tweet['city'] is None) or (tweet['country'] is None)):
	return None

    # time related fields
    tweet['time_ms'] = long(item['timestamp_ms'])
    tweetTime = datetime.datetime.fromtimestamp(tweet['time_ms']/1000)
    tweet['minuteSlot'] = convert_to_1m(tweetTime)
    tweet['hourSlot'] = convert_to_1h(tweetTime)
    tweet['daySlot'] = convert_to_1d(tweetTime)

#    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet


# main work...
conf = (SparkConf().setAppName("Tweets-Py-mvp"))
sc = SparkContext(conf = conf)
tweet_jsons = sc.textFile(folder_in)

tweets = tweet_jsons.map(tweet_from_json_line).persist()

# location related work
tweetsWithCoords = tweets.filter(lambda tweet : (tweet is not None) and ('coords' in tweet) and (len(tweet['coords']) > 0)).persist()

tweetLocationsCity = tweetsWithCoords.map(lambda tweet : (tweet['country'], tweet['city'], tweet['time_ms'], tweet['coords'][1], tweet['coords'][0]))
tweetLocationsCity.saveToCassandra(keyspace, "tweet_locations_city") # key: ((country, city), time_ms)

tweetLocationsCountry = tweetsWithCoords.map(lambda tweet : (tweet['country'], tweet['time_ms'], tweet['coords'][1], tweet['coords'][0]))
tweetLocationsCountry.saveToCassandra(keyspace, "tweet_locations_country") # key: (country, time_ms)

# tag related work
tweetsWithTags = tweets.filter(lambda tweet : (tweet is not None) and ('tags' in tweet) and (len(tweet['tags']) > 0))

tagsAsValue = tweetsWithTags.map(lambda tweet : ((tweet['daySlot'], tweet['hourSlot'], tweet['minuteSlot'], tweet['country'], tweet['city']), tweet['tags']))
singleTagTuples = tagsAsValue.flatMapValues(lambda x : x).persist() # ((daySlot, hourSlot, minuteSlot, country, city), tag)

# city 
tagCityDailyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((d, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
cdt = tagCityDailyCount.map(lambda ((d, cc, c, t), count) : (d, cc, c, t, count)) # flatMap(lambda x : x).saveToCassandra didn't work...
cdt.saveToCassandra(keyspace, "city_day_trends") # key: ((dayslot, country, city), topic)
#top_cdt = sc.parallelize(cdt.takeOrdered(topCount, key = lambda x: -x[4])) # takeOrdered returns a list, not an RDD, so need to parallelize
#top_cdt.saveToCassandra(keyspace, "city_day_top_trends") # top trends only

tagCityHourlyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((h, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
cht = tagCityHourlyCount.map(lambda ((h, cc, c, t), count) : (h, cc, c, t, count))
cht.saveToCassandra(keyspace, "city_hour_trends") # key: ((hourslot, country, city), topic)
#top_cht = sc.parallelize(cht.takeOrdered(topCount, key = lambda x: -x[4]))
#top_cht.saveToCassandra(keyspace, "city_hour_top_trends") # top hourly topics

tagCityMinuteCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((m, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
cmt = tagCityMinuteCount.map(lambda ((m, cc, c, t), count) : (m, cc, c, t, count))
cmt.saveToCassandra(keyspace, "city_minute_trends") # key: ((minuteslot, country, city), topic)
#tagCityMinuteCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "city_minute_top_trends")

# country
tagCountryDailyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((d, cc, t), 1)).reduceByKey(lambda x, y: x + y) 
ccdc = tagCountryDailyCount.map(lambda ((d, cc, t), count) : (d, cc, t, count))
ccdc.saveToCassandra(keyspace, "country_day_trends") # key: ((dayslot, country), topic)
#tagCountryDailyCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "country_day_top_trends")

tagCountryHourlyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((h, cc, t), 1)).reduceByKey(lambda x, y: x + y) 
cchc = tagCountryHourlyCount.map(lambda ((h, cc, t), count) : (h, cc, t, count))
cchc.saveToCassandra(keyspace, "country_hour_trends") # key: ((hourslot, country), topic)
#tagCountryHourlyCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "country_hour_top_trends")

tagCountryMinuteCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((m, cc, t), 1)).reduceByKey(lambda x, y: x + y) 
cchc = tagCountryMinuteCount.map(lambda ((m, cc, t), count) : (m, cc, t, count))
cchc.saveToCassandra(keyspace, "country_minute_trends") # key: ((minuteslot, country), topic)
#tagCountryMinuteCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "country_minute_top_trends")

# world
tagWorldDailyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((d, t), 1)).reduceByKey(lambda x, y: x + y) 
wdc = tagWorldDailyCount.map(lambda ((d, t), count) : (d, t, count))
wdc.saveToCassandra(keyspace, "world_day_trends") # key: (dayslot, topic)
#tagWorldDailyCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "world_day_top_trends")

tagWorldHourlyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((h, t), 1)).reduceByKey(lambda x, y: x + y) 
whc = tagWorldHourlyCount.map(lambda ((h, t,), count) : (h, t, count))
whc.saveToCassandra(keyspace, "world_hour_trends") # key: (hourslot, topic)
#tagWorldHourlyCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "world_hour_trends")

tagWorldMinuteCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((m, t), 1)).reduceByKey(lambda x, y: x + y) 
wmc = tagWorldMinuteCount.map(lambda ((m, t), count) : (m, t, count))
wmc.saveToCassandra(keyspace, "world_minute_trends") # key: (minuteslot, topic)
#tagWorldMinuteCount.takeOrdered(topCount, key = lambda x: -x[1]).saveToCassandra(keyspace, "world_minute_trends")

# 
'''
folder_out = folder_in + "_out2_"
tagCityDailyCount.saveAsTextFile(folder_out+"D_city")
tagCityHourlyCount.saveAsTextFile(folder_out+"H_city")
tagCityMinuteCount.saveAsTextFile(folder_out+"M_city")
tagCountryDailyCount.saveAsTextFile(folder_out+"D_country")
tagCountryHourlyCount.saveAsTextFile(folder_out+"H_country")
tagCountryMinuteCount.saveAsTextFile(folder_out+"M_country")
tagWorldDailyCount.saveAsTextFile(folder_out+"D_world")
tagWorldHourlyCount.saveAsTextFile(folder_out+"H_world")
tagWorldMinuteCount.saveAsTextFile(folder_out+"M_world")
#singleTagTuples.saveAsTextFile(folder_out)
'''
