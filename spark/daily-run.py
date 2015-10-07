from pyspark import SparkContext, SparkConf
import json
import sys
import datetime

import pyspark_cassandra
from operator import itemgetter, attrgetter

hdfs = "hdfs://172.31.25.203:9000"
path = ""
keyspace = "trends"
topCount = 20 # to be used when inserting into top tables -- will spead up the reads later

# can take the path to a folder/file as an argument to be added to the hdfs path
# expecting a path to an hourly folder to run the hourly script on
if len(sys.argv) > 1:
    path = sys.argv[1]
else:
    print("Expecting the HDFS path as an argument")
    exit()

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
#    tweet['minuteSlot'] = convert_to_1m(tweetTime)
#    tweet['hourSlot'] = convert_to_1h(tweetTime)
    tweet['daySlot'] = convert_to_1d(tweetTime)

#    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet


# main work...
conf = (SparkConf().setAppName("Tweets-Daily"))
sc = SparkContext(conf = conf)
tweet_jsons = sc.textFile(folder_in)

tweets = tweet_jsons.map(tweet_from_json_line).persist()

totalTweetCount = tweets.count()
tweet = tweets.first()
dayslot = tweet['daySlot']

# tag related work
tweetsWithTags = tweets.filter(lambda tweet : (tweet is not None) and ('tags' in tweet) and (len(tweet['tags']) > 0))

taggedTweetCount = tweetsWithTags.count()

tagsAsValue = tweetsWithTags.map(lambda tweet : ((tweet['daySlot'],  tweet['country'], tweet['city']), tweet['tags']))
singleTagTuples = tagsAsValue.flatMapValues(lambda x : x).persist() # ((daySlot, hourSlot, minuteSlot, country, city), tag)

# city 
tagCityDailyCount = singleTagTuples.map(lambda ((d, cc, c), t) : ((d, cc, c, t), 1)).reduceByKey(lambda x, y: x + y).persist()
cdt = tagCityDailyCount.map(lambda ((d, cc, c, t), count) : (d, cc, c, t, count))
cdt.saveToCassandra(keyspace, "city_day_trends") # key: ((dayslot, country, city), topic)
# top trends per city
top_cdt_packed = tagCityDailyCount.map(lambda ((d, cc, c, t), count) : ((d, cc, c), (t, count))).groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=itemgetter(1), reverse=True)[:topCount]))
top_cdt = top_cdt_packed.flatMapValues(lambda x : x)
top_cdt.saveToCassandra(keyspace, "city_day_top_trends") # top trends only

# country
tagCountryDailyCount = singleTagTuples.map(lambda ((d, cc, c), t) : ((d, cc, t), 1)).reduceByKey(lambda x, y: x + y).persist()
ccdc = tagCountryDailyCount.map(lambda ((d, cc, t), count) : (d, cc, t, count))
ccdc.saveToCassandra(keyspace, "country_day_trends") # key: ((dayslot, country), topic)
# top trends per country
top_ccdt_packed = tagCountryDailyCount.map(lambda ((d, cc, t), count) : ((d, cc), (t, count))).groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=itemgetter(1), reverse=True)[:topCount]))
top_ccdt = top_ccdt_packed.flatMapValues(lambda x : x)
top_ccdt.saveToCassandra(keyspace, "country_day_top_trends") # top trends only


# world
tagWorldDailyCount = singleTagTuples.map(lambda ((d, cc, c), t) : ((d, t), 1)).reduceByKey(lambda x, y: x + y) 
wdc = tagWorldDailyCount.map(lambda ((d, t), count) : (d, t, count))
wdc.saveToCassandra(keyspace, "world_day_trends") # key: (dayslot, topic)

totalTrendCount = wdc.count()
counts = sc.parallelize([{"dayslot":dayslot, "count_type":"total", "count":totalTweetCount}, 
			{"dayslot":dayslot, "count_type":"tagged", "count":taggedTweetCount},
			{"dayslot":dayslot, "count_type":"trends", "count":totalTrendCount}])
counts.saveToCassandra(keyspace, "world_day_counts")

# top trends for the day -- all locations
top_wdc = sc.parallelize(wdc.takeOrdered(topCount, key = lambda x: -x[2]))
top_wdc.saveToCassandra(keyspace, "world_day_top_trends")

