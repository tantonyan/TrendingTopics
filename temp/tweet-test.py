from pyspark import SparkContext, SparkConf
import json
import sys
import datetime

hdfs = "hdfs://ec2-54-209-187-157.compute-1.amazonaws.com:9000"
#path = "/user/test-tweets/tweets-17-22.json"
path = "/user/test-tweets/json-tweets-10.txt"
# can take the path to a folder/file as an argument to be added to the hdfs path
if len(sys.argv) > 1:
    path = sys.argv[1]

folder_in = hdfs + path

def convert_to_1hr(time_ms):
    return datetime.datetime.fromtimestamp(long(time_ms)/1000).strftime('%Y%m%d%H')

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
    tweet['time_ms'] = item['timestamp_ms']
    tweet['hourSlot'] = convert_to_1hr(tweet['time_ms'])
    tweet['u_id'] = userId(item) # just the userId is enough
    tweet['coors'] = item['coordinates'] # list [latitude,longitude]
    tweet['tags'] = trendItems(item)
    tweet['city'] = tweetCity(item)
    tweet['country'] = tweetCountry(item) # the country code: US, UK, etc.
#    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet


# main work...
conf = (SparkConf().setAppName("Tweets-Py-mvp"))
sc = SparkContext(conf = conf)
tweet_jsons = sc.textFile(folder_in)

tweets = tweet_jsons.map(tweet_from_json_line)#.persist

tweetsWithTags = tweets.filter(lambda tweet : len(tweet['tags']) > 0)

tagsAsValue = tweetsWithTags.map(lambda tweet : ((tweet['hourSlot'], tweet['country'], tweet['city']), tweet['tags']))
singleTagTuples = tagsAsValue.flatMapValues(lambda x : x) # ((hourSlot, country, city), tag)

tagPlaceHourlyCount = singleTagTuples.map(lambda ((h, cc, c), t) : ((h, cc, c, t), 1)).reduceByKey(lambda x, y: x + y) 
	
folder_out = folder_in + "_pyOut2"
tagPlaceHourlyCount.saveAsTextFile(folder_out)
#singleTagTuples.saveAsTextFile(folder_out)
'''
allTweets = tweets.map(lambda tweet : (tweet['hourSlot'], tweet['country'], tweet['city'], tweet['tags']))
allTweets.saveAsTextFile(folder_out+"_all")
wtTweets = tweetsWithTags.map(lambda tweet : (tweet['hourSlot'], tweet['country'], tweet['city'], tweet['tags']))
wtTweets.saveAsTextFile(folder_out+"_wt")
'''
