import json
import datetime

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
    tweet['hourSlot'] = convert_to_1h(tweetTime)
#    tweet['daySlot'] = convert_to_1d(tweetTime)

#    tweet['text'] = item['text'] # don't need the actual tweet...

    return tweet
