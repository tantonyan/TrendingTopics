#!/bin/bash

sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

# test if it is running
#kafka-topics --describe --topic my-topic
