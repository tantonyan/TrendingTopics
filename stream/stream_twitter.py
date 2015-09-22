# This scrip will take the following from a twitter stream and insert it into kafka as a one line json object:
#t_id	time_ms	coords	hashtags	u_ment	u_id	u_loc	pl_type	pl_name	pl_cc	pl_full_name	text

import sys
import json

from TwitterAPI import TwitterAPI 
# modified /usr/local/lib/python2.7/dist-packages/TwitterAPI/TwitterAPI.py

from kafka import SimpleProducer, KafkaClient
import constants # API keys/secrets

if len(sys.argv) > 1:
    topic = sys.argv[1]
else:
    print("need a kafka topic as an argument")
    exit()

print("using topic: " + topic)

# To send messages synchronously
kafka = KafkaClient('localhost:9092')
#partitionIds = kafka.get_partition_ids_for_topic(topic)
producer = SimpleProducer(kafka)

LOCATION = '-180,-90,180,90' # the world!

api = TwitterAPI(constants.CONSUMER_KEY,
                 constants.CONSUMER_SECRET,
                 constants.ACCESS_TOKEN_KEY,
                 constants.ACCESS_TOKEN_SECRET)
r = api.request('statuses/filter', {'locations': LOCATION})

# get the coords from the twitter item
def coords(item):
    coords = []
    if ('coordinates' in item) and isinstance(item['coordinates'], dict):
	# only continue if we have a list (could be 'None')
        if 'coordinates' in item['coordinates']:
            coords = item['coordinates']['coordinates'] # the actual coords of the user

    return coords
    
# get the place info for a twitter item
def place(item):
    place = {}

    if ('place' in item) and isinstance(item['place'], dict):
	# only continue if we have a list (could be 'None')
	if 'place_type' in item['place']:
	    place['place_type'] = item['place']['place_type']

	if 'name' in item['place']:
	    place['name'] = item['place']['name']

	if 'country_code' in item['place']:
	    place['country_code'] = item['place']['country_code']

	if 'full_name' in item['place']:
	    place['full_name'] = item['place']['full_name']

    return place

# get just the hashtags and send back as a list
def getHashtags(hTags):
    tags = []
    if (not isinstance(hTags, list)) or (len(hTags) == 0):
	return tags

    for tag in hTags:
	# each tag usually looks like: {u'indices': [64, 72], u'text': u'perfect'}
	if isinstance(tag, dict) and ('text' in tag):
	    tags.append(tag['text'])

    return tags 

# get just the screen names of the mentioned users
def getUsers(uMentions):
    users = []
    if (not isinstance(uMentions, list)) or (len(uMentions) == 0):
	return users

    for user in uMentions:
	# each user mention object usually looks like: 
        # {u'id': 436292177, u'indices': [0, 14], u'id_str': u'436292177', u'screen_name': u'JamesFrancoTV', u'name': u'James Franco'}
	if isinstance(user, dict) and ('screen_name' in user):
	    users.append(user['screen_name'])

    return users


# get hashtag and user mentions from the twitter item
def entities(item):
    ent = {'hashtags':[],'user_mentions':[]}
    if ('entities' in item) and isinstance(item['entities'], dict):
	# only continue if we have a list (could be 'None')
	if 'hashtags' in item['entities']:
	    ent['hashtags'] = getHashtags(item['entities']['hashtags'])

	if 'user_mentions' in item['entities']:
	    ent['user_mentions'] = getUsers(item['entities']['user_mentions'])

    return ent

# get poster data for a given twitter item
def userData(item):
    uData = {}
    if ('user' in item) and isinstance(item['user'], dict):
	if 'id_str' in item['user']:
	    uData['id_str'] = item['user']['id_str']
	
	if 'id_str' in item['user']:
	    uData['location'] = item['user']['location']

    return uData

# clean up the message and produce a cleaned up line to be used for kafka
def kafkaItem(item):
    line = {}
    if ('id_str' not in item) or ('timestamp_ms' not in item) or('text' not in item): 
	# other message (limit, etc.)
	print("Received a non-tweet item: \n\t"+str(item))
        return line 

    try:
	line['id_str'] = item['id_str'] # expected to be present
	line['timestamp_ms'] = item['timestamp_ms'] # expected to be present
	line['coordinates'] = coords(item)
	line['entities'] = entities(item)
	line['user'] = userData(item)
	line['place'] = place(item)
	line['text'] = item['text'] # in case we need to mine it later

    except: # in case there are issues just send back an empty string
	print("Exception: " + str(sys.exc_info()[0]) + "\n On: \n\t" + str(line) + " From \n\t" + str(item))
	return ''

    return line

# raw iterable stream object:
#part_num = len(partitionIds)
#part_id = 0 # we will specify the partition to be used -- makes easier for kafka
item_enc = ''
for item in r:
#    print(item)
    kItem = kafkaItem(item)
    try:
	item_enc = json.dumps(kItem).encode('UTF-8') # producer expects the message to be in b type
    except: # sometimes we get an encoding exception 
	    # e.g. "UnicodeDecodeError: 'ascii' codec can't decode byte 0xe2"
	print("encoding exception...")
	continue # don't process this tweet if there were issues with encoding 

    producer.send_messages(topic, item_enc) 
#    producer.send_messages(topic, part_id, item_enc) 
#    part_id = (part_id + 1) % part_num

