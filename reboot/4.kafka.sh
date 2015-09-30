#!/bin/bash

sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

sleep 5

$KAFKA_MANAGER_HOME/bin/kafka-manager -Dhttp.port=9001 &
# test if it is running
#kafka-topics --describe --topic my-topic
