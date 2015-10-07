from pyleus.storm import SimpleBolt
import json
import datetime
import time
from cassandra.cluster import Cluster

# get poster's user
def userId(item):
    if ('user' in item) and isinstance(item['user'], dict):
	if 'id_str' in item['user']:
	    return item['user']['id_str']
    return None
# get poster's screen_name 
def userName(item):
    uName = "Anonymous"
    if ('user' in item) and isinstance(item['user'], dict):
	if 'screen_name' in item['user']:
	    return item['user']['screen_name']
	else:
	    uName = userId(item)
    if (uName is not None):
	return uName
    return "Anonymous"

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
#    item = json_line

    try:
        item = json.loads(json_line)
    except:
	return None # not a json line -- just retun None

    if ('id_str' not in item) or ('timestamp_ms' not in item) or('text' not in item):
	return None # not what we expected

    tweet = {}
    tweet['t_id'] = item['id_str']
    tweet['u_id'] = userId(item) # just the userId is enough
    tweet['userName'] = userName(item) # just the userName is enough
    tweet['coords'] = item['coordinates'] # list [longitude,latitude]
    tweet['tags'] = trendItems(item) # will have the combined list of hashtags and user mentions
    tweet['city'] = tweetCity(item)
    tweet['country'] = tweetCountry(item) # the country code: US, UK, etc.
    tweet['time_ms'] = long(item['timestamp_ms'])

    # if any of the fields are missing return None as well
    if ((tweet['u_id'] is None) or (tweet['city'] is None) or (tweet['country'] is None)):
	return None

    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet


cluster = Cluster(['172.31.46.91', '172.31.46.92', '172.31.46.93'])
session = cluster.connect('trends')

# insert into the location tables
def insert_locations(tweet):
    cql_insert = """INSERT INTO rt_tweet_locations_world (time_ms, lat, long)
		    VALUES (%s, %s, %s)"""
    session.execute(cql_insert, [tweet['time_ms'], float(tweet['coords'][1]), float(tweet['coords'][0])])

def insert_tweet_text(tweet, topic):
    cql_insert = """INSERT INTO rt_tweet_world (topic, user, time_ms, tweet)
		    VALUES (%s, %s, %s, %s)"""
    session.execute(cql_insert, [topic, tweet['userName'], tweet['time_ms'], tweet['text']])

class JsonSplitterBolt(SimpleBolt):

    OUTPUT_FIELDS = ['time_ms', 'country', 'city', 'topic']

    def process_tuple(self, tup):
        json_tweet, = tup.values
#	self.emit(['From Bolt', str(len(json_tweet)), str(type(json_tweet)), '---------------------'])
        tweet = tweet_from_json_line(json_tweet)
        if (tweet is None) or (tweet['time_ms'] + 2000 < (time.time() * 1000L)): # only recent (within last 2 secnds) tweets
#	    self.emit(['From Bolt', "--- --- None --- ---", " --- --- parsing error ---", '---------------------'])
	    return

	if len(tweet['coords']) > 0: # tweet with a location found
	    insert_locations(tweet)

	if len(tweet['tags']) > 0: # tweet with a topic(s) found
	    for topic in tweet['tags']:
		insert_tweet_text(tweet, topic)
		# send out the general info for the tick tuple process to combine
        	self.emit((tweet['time_ms'], tweet['country'], tweet['city'], topic), anchors=[tup])

if __name__ == '__main__':
    JsonSplitterBolt().run()
