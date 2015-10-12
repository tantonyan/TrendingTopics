from pyleus.storm import SimpleBolt
import datetime
import time
from cassandra.cluster import Cluster
from operator import itemgetter#, attrgetter

import logging
import logging.config

cluster = Cluster(['172.31.46.93', '172.31.46.92', '172.31.46.91'])
session = cluster.connect('trends')

log = logging.getLogger("minute-bolt")
toLog = True

topCount = 20

def insert_cql(cql_stmt, params): # wrapper to catch exceptions
    try:
	session.execute(cql_stmt, params)
    except:
	e = sys.exc_info()[0]
	log.exception("Exception on insert: " + e + "\n\t" + cql_stmt + str(params))
	

def insert_trends_world(minuteslot, records): # records is an array of (topic, count)
    cql_insert = """INSERT INTO world_minute_trends (minuteslot, topic, count)
		    VALUES (%s, %s, %s)"""
    sorted_r = sorted(records.items(), key=itemgetter(1), reverse=True)[:topCount]
    for topic, count in sorted_r:
      insert_cql(cql_insert, [minuteslot, topic, count])

def insert_trends_country(minuteslot, records): # records is an array of (country, topic, count)
    cql_insert = """INSERT INTO country_minute_trends (minuteslot, country, topic, count)
		    VALUES (%s, %s, %s, %s)"""
    for record, count in records.iteritems():
      insert_cql(cql_insert, [minuteslot, record[0], record[1], count])

def insert_trends_city(minuteslot, records): # records is an array of (country, city, topic, count)
    cql_insert = """INSERT INTO city_minute_trends (minuteslot, country, city, topic, count)
		    VALUES (%s, %s, %s, %s, %s)"""
    for record, count in records.iteritems():
      insert_cql(cql_insert, [minuteslot, record[0], record[1], record[2], count])

    
class MinuteBolt(SimpleBolt):

    def initialize(self):
	self.worldTrends = {}
	self.countryTrends = {}
	self.cityTrends = {}
	minuteslot = long(time.strftime('%Y%m%d%H%M'))

    def process_tuple(self, tup):
        time_ms,country,city,topic = tup.values
        if (time_ms is None) or (country is None) or (city is None) or (topic is None): 
	    return

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

    def process_tick(self):
	if toLog:
	    log.info("Processing a tick: sizes=%d, %d, %d", len(self.worldTrends), len(self.countryTrends), len(self.cityTrends))

	self.minuteslot = long(time.strftime('%Y%m%d%H%M'))
	insert_trends_city(self.minuteslot, self.cityTrends)
	insert_trends_country(self.minuteslot, self.countryTrends)
	insert_trends_world(self.minuteslot, self.worldTrends)

	self.worldTrends.clear()
	self.countryTrends.clear()
	self.cityTrends.clear()

	if toLog:
	    log.info("Cleared state: sizes=%d, %d, %d", len(self.worldTrends), len(self.countryTrends), len(self.cityTrends))

if __name__ == '__main__':
    MinuteBolt().run()
