from pyleus.storm import SimpleBolt
import datetime
import time
from cassandra.cluster import Cluster

cluster = Cluster(['172.31.46.93', '172.31.46.92', '172.31.46.91'])
session = cluster.connect('trends')

def insert_trends_world(minuteslot, records): # records is an array of (topic, count)
    cql_insert = """INSERT INTO world_minute_trends (minuteslot, topic, count)
		    VALUES (%s, %s, %s)"""
    for topic, count in records.iteritems():
      session.execute(cql_insert, [minuteslot, topic, count])

def insert_trends_country(minuteslot, records): # records is an array of (country, topic, count)
    cql_insert = """INSERT INTO country_minute_trends (minuteslot, country, topic, count)
		    VALUES (%s, %s, %s, %s)"""
    for record, count in records.iteritems():
      session.execute(cql_insert, [minuteslot, record[0], record[1], count])

def insert_trends_city(minuteslot, records): # records is an array of (country, city, topic, count)
    cql_insert = """INSERT INTO city_minute_trends (minuteslot, country, city, topic, count)
		    VALUES (%s, %s, %s, %s, %s)"""
    for record, count in records.iteritems():
      session.execute(cql_insert, [minuteslot, record[0], record[1], record[2], count])

    
class MinuteBolt(SimpleBolt):

    def initialize(self):
	self.worldTrends = {}
	self.countryTrends = {}
	self.cityTrends = {}
	minuteslot = long(time.strftime('%Y%m%d%H%M'))

    def log_status(self):
      with open('/home/ubuntu/temp/minute-bolt.json', 'a') as f:
	f.write("------ " + str(self.minuteslot) + " -------")
	f.write(str(self.worldTrends))
	f.write(str(self.countryTrends))
	f.write(str(self.cityTrends))
      f.close()

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
	self.minuteslot = long(time.strftime('%Y%m%d%H%M'))
	insert_trends_city(self.minuteslot, self.cityTrends)
	insert_trends_country(self.minuteslot, self.countryTrends)
	insert_trends_world(self.minuteslot, self.worldTrends)
#	self.log_status()
	self.worldTrends.clear()
	self.countryTrends.clear()
	self.cityTrends.clear()

if __name__ == '__main__':
    MinuteBolt().run()
