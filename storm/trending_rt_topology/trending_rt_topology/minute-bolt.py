from pyleus.storm import SimpleBolt
import datetime
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, PreparedStatement
from operator import itemgetter#, attrgetter
import sys

import logging
import logging.config

cluster = Cluster(['172.31.46.93', '172.31.46.92', '172.31.46.91'])
session = cluster.connect('trends')


cql_world_insert = "INSERT INTO world_minute_trends (minuteslot, topic, count) VALUES (?, ?, ?)"
cql_country_insert = "INSERT INTO country_minute_trends (minuteslot, country, topic, count) VALUES (?, ?, ?, ?)"
cql_city_insert = "INSERT INTO city_minute_trends (minuteslot, country, city, topic, count) VALUES (?, ?, ?, ?, ?)"

cql_world_stmt = session.prepare(cql_world_insert)
cql_country_stmt = session.prepare(cql_country_insert)
cql_city_stmt = session.prepare(cql_city_insert)

batch_limit_world = 70 # will be capped by topCount (20) anyway
batch_limit_country = 50
batch_limit_city = 30

log = logging.getLogger("minute-bolt")
toLog = False

topCount = 20

def execBatch(batch): # simple wrapper to run the batch executions and log exceptions
    try:
	session.execute(batch)
    except:
	e = sys.exc_info()[0]
	log.exception("Exception on batch insert: " + str(e))

def insert_cql(cql_prep_stmt, params): # wrapper to catch exceptions
    try:
	session.execute_async(cql_prep_stmt, params)
    except:
	e = sys.exc_info()[0]
	log.exception("Exception on insert: " + str(e) + "\n\t" + cql_stmt + str(params))
	

def insert_trends_world(minuteslot, records): # records is an array of (topic, count)
    sorted_r = sorted(records.items(), key=itemgetter(1), reverse=True)[:topCount]
    count = 0
    batch = BatchStatement()
    for topic, count in sorted_r:
      '''
      insert_cql(cql_world_stmt, (minuteslot, topic, count))
      '''
      batch.add(cql_world_stmt, (minuteslot, topic, count))
      count += 1
      if (count == batch_limit_world):
        execBatch(batch)
        count = 0
        batch = BatchStatement()

    if (count > 0):
      execBatch(batch)
def insert_trends_country(minuteslot, records): # records is an array of (country, topic, count)
    count = 0
    batch = BatchStatement()
    for record, count in records.iteritems():
      '''
      insert_cql(cql_country_stmt, (minuteslot, record[0], record[1], count))
      '''
      batch.add(cql_country_stmt, (minuteslot, record[0], record[1], count))
      count += 1
      if (count == batch_limit_country):
        execBatch(batch)
        count = 0
        batch = BatchStatement()

    if (count > 0):
      execBatch(batch)
def insert_trends_city(minuteslot, records): # records is an array of (country, city, topic, count)
    count = 0
    batch = BatchStatement()
    for record, count in records.iteritems():
      '''
      insert_cql(cql_city_stmt, (minuteslot, record[0], record[1], record[2], count))
      '''
      batch.add(cql_city_stmt, (minuteslot, record[0], record[1], record[2], count))
      count += 1
      if (count == batch_limit_city):
        execBatch(batch)
        count = 0
        batch = BatchStatement()

    if (count > 0):
      execBatch(batch)
    
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
