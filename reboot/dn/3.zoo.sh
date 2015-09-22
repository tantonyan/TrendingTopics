#!/bin/bash

sudo /usr/local/zookeeper/bin/zkServer.sh start

# check if it's running:
echo srvr | nc localhost 2181 | grep Mode
