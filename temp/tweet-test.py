from pyspark import SparkContext, SparkConf
import json
import sys
import datetime

hdfs = "hdfs://ec2-54-209-187-157.compute-1.amazonaws.com:9000"
path = "/user/test-tweets/tweets-17-22.json"
#path = "/user/test-tweets/json-tweets-10.txt"
# can take the path to a folder/file as an argument to be added to the hdfs path
if len(sys.argv) > 1:
    path = sys.argv[1]

folder_in = hdfs + path

# take a datetime object and return a string for the minute slot
def convert_to_1m(dt):
    return dt.strftime('%Y%m%d%H%M')

# take a datetime object and return a string for the hour slot
def convert_to_1h(dt):
    return dt.strftime('%Y%m%d%H')

# take a datetime object and return a string for the day slot
def convert_to_1d(dt):
    return dt.strftime('%Y%m%d')

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
    tweet['coors'] = item['coordinates'] # list [latitude,longitude]
    tweet['tags'] = trendItems(item) # will have the combined list of hashtags and user mentions
    tweet['city'] = tweetCity(item)
    tweet['country'] = tweetCountry(item) # the country code: US, UK, etc.

    # time related fields
    tweet['time_ms'] = item['timestamp_ms']
    tweetTime = datetime.datetime.fromtimestamp(long(tweet['time_ms'])/1000)
    tweet['minuteSlot'] = convert_to_1m(tweetTime)
    tweet['hourSlot'] = convert_to_1h(tweetTime)
    tweet['daySlot'] = convert_to_1d(tweetTime)

#    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet


# main work...
conf = (SparkConf().setAppName("Tweets-Py-mvp"))
sc = SparkContext(conf = conf)
tweet_jsons = sc.textFile(folder_in)

tweets = tweet_jsons.map(tweet_from_json_line)#.persist

tweetsWithTags = tweets.filter(lambda tweet : (tweet is not None) and ('tags' in tweet) and (len(tweet['tags']) > 0))

tagsAsValue = tweetsWithTags.map(lambda tweet : ((tweet['daySlot'], tweet['hourSlot'], tweet['minuteSlot'], tweet['country'], tweet['city']), tweet['tags']))
singleTagTuples = tagsAsValue.flatMapValues(lambda x : x)#.persist # ((daySlot, hourSlot, minuteSlot, country, city), tag)

tagPlaceDailyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((d, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
tagPlaceHourlyCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((h, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
tagPlaceMinuteCount = singleTagTuples.map(lambda ((d, h, m, cc, c), t) : ((m, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
	
folder_out = folder_in + "_pyOut"
tagPlaceDailyCount.saveAsTextFile(folder_out+"D")
tagPlaceHourlyCount.saveAsTextFile(folder_out+"H")
tagPlaceMinuteCount.saveAsTextFile(folder_out+"M")
#singleTagTuples.saveAsTextFile(folder_out)
