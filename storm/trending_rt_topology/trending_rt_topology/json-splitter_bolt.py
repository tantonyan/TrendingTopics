from pyleus.storm import SimpleBolt
import json
import datetime
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

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

def insert_locations(batch, tweet):
    cql_prepare = "INSERT INTO rt_tweet_locations_world (time_ms, lat, long) VALUES (?, ?, ?)"
    loc_stmt = session.prepare(cql_prepare)

    batch.add(cql_prepare, [tweet['time_ms'], float(tweet['coords'][1]), float(tweet['coords'][0])])

def insert_tweet_text(batch, tweet, topic):
    cql_prepare = "INSERT INTO rt_tweet_world (topic, user, time_ms, tweet) VALUES (?, ?, ?, ?)"
    tweet_stmt = session.prepare(cql_prepare)

    batch.add(cql_prepare, [topic, tweet['userName'], tweet['time_ms'], tweet['text']])

def batch_trends_world(batch, minuteslot, records): # records is an array of (topic, count)
    cql_prepare = "INSERT INTO world_minute_trends (minuteslot, topic, count) VALUES (?, ?, ?)"
    world_stmt = session.prepare(cql_prepare)

    for topic, count in records.iteritems():
      batch.add(world_stmt, [minuteslot, topic, count])

def batch_trends_country(batch, minuteslot, records): # records is an array of (country, topic, count)
    cql_prepare = "INSERT INTO country_minute_trends (minuteslot, country, topic, count) VALUES (?, ?, ?, ?)"
    country_stmt = session.prepare(cql_prepare)

    for record, count in records.iteritems():
      batch.add(country_stmt, [minuteslot, record[0], record[1], count])

def batch_trends_city(batch, minuteslot, records): # records is an array of (country, city, topic, count)
    cql_prepare = "INSERT INTO city_minute_trends (minuteslot, country, city, topic, count) VALUES (?, ?, ?, ?, ?)"
    city_stmt = session.prepare(cql_prepare)

    for record, count in records.iteritems():
      batch.add(city_stmt, [minuteslot, record[0], record[1], record[2], count])


class JsonSplitterBolt(SimpleBolt):

#    OUTPUT_FIELDS = ['time_ms', 'country', 'city', 'topic']
    def initialize(self):
	self.worldTrends = {}
	self.countryTrends = {}
	self.cityTrends = {}
	self.minuteslot = long(time.strftime('%Y%m%d%H%M'))
	self.stmtCounter = 0
	self.batchS = BatchStatement() # frequent batch
	self.batchM = BatchStatement() # minute batch

    # save/make the minute based dictionaries
    def build_minute_items(self, country, city, topic):
	cityKey = (country, city, topic)
	countryKey = (country, topic)
	worldKey = topic

	if cityKey in self.cityTrends:
	    self.cityTrends[cityKey] += 1
	else:
	    self.cityTrends[cityKey] = 1

	if countryKey in self.countryTrends:
	    self.countryTrends[countryKey] += 1
	else:
	    self.countryTrends[countryKey] = 1

	if worldKey in self.worldTrends:
	    self.worldTrends[worldKey] += 1
	else:
	    self.worldTrends[worldKey] = 1

    def process_tuple(self, tup):
        json_tweet, = tup.values
        tweet = tweet_from_json_line(json_tweet)
        if (tweet is None) or (tweet['time_ms'] + 2000 < (time.time() * 1000L)): # only recent (within last 2 secnds) tweets
	    return

	if len(tweet['coords']) > 0: # tweet with a location found
	    insert_locations(self.batchS, tweet)
	    self.stmtCounter += 1
	    if (self.stmtCounter == 20):
		session.execute(self.batchS)
		self.stmtCounter = 0

	if len(tweet['tags']) > 0: # tweet with a topic(s) found
	    for topic in tweet['tags']:
		insert_tweet_text(self.batchS, tweet, topic)
	        self.stmtCounter += 1
	        if (self.stmtCounter == 20):
		    session.execute(self.batchS)
		    self.stmtCounter = 0

		self.build_minute_items(tweet['country'], tweet['city'], topic)
#        	self.emit((tweet['time_ms'], tweet['country'], tweet['city'], topic), anchors=[tup])

    def process_tick(self):
	self.minuteslot = long(time.strftime('%Y%m%d%H%M'))

	batch_trends_city(self.batchM, self.minuteslot, self.cityTrends)
	batch_trends_country(self.batchM, self.minuteslot, self.countryTrends)
	batch_trends_world(self.batchM, self.minuteslot, self.worldTrends)

	session.execute(self.batchM)
	self.worldTrends.clear()
	self.countryTrends.clear()
	self.cityTrends.clear()

if __name__ == '__main__':
    JsonSplitterBolt().run()
