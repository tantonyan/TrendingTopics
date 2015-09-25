import json
import datetime
from cassandra.cluster import Cluster

json_path = "/home/ubuntu/temp/tweets/tweets-17-22.json"
cassandraIP = '172.31.46.91'
keyspace = 'trends'

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
    tweet['coords'] = item['coordinates'] # list [latitude,longitude]
    tweet['tags'] = trendItems(item) # will have the combined list of hashtags and user mentions
    tweet['city'] = tweetCity(item)
    tweet['country'] = tweetCountry(item) # the country code: US, UK, etc.

    # if any of the fields are missing return None as well
    if ((tweet['u_id'] is None) or (tweet['city'] is None) or (tweet['country'] is None)):
	return None

    # time related fields
    tweet['time_ms'] = long(item['timestamp_ms'])
#    tweetTime = datetime.datetime.fromtimestamp(long(tweet['time_ms'])/1000)
#    tweet['minuteSlot'] = convert_to_1m(tweetTime)
#    tweet['hourSlot'] = convert_to_1h(tweetTime)
#    tweet['daySlot'] = convert_to_1d(tweetTime)

#    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet


f_in = open(json_path, "r")
tweets = []

for jsonLine in f_in:
    t = tweet_from_json_line(jsonLine)
    if t is not None:
        tweets.append(t)

cluster = Cluster([cassandraIP])
session = cluster.connect(keyspace)

print(str(len(tweets)) + " tweets: will insert into Cassandra now...")

for tweet in tweets:
    if (len(tweet['coords']) > 0):
        session.execute(
            """
            INSERT INTO tweet_locations (country, city, time_ms, lat, long)
            VALUES (%s, %s, (%s, %s), %s)
            """,
            (tweet['country'], tweet['city'], tweet['time_ms'], tweet['coords'][1], tweet['coords'][0])
        )


f_in.close()
