#!/bin/bash

#topic="twitter-json"
topic="twitter-json3"

log="stream.log"

cd ~/stream

# the streaming script at times stops with no apparent reason
# just put it in a while true loop to restart if that happens
# we need to collect as much of the stream as possible.
while true
do
    echo "-- -- -- -- -- --" >> $log
    echo `date` >> $log
    python stream_twitter.py $topic >> $log 2>&1
    echo `date` >> $log
    echo "-- -- -- -- -- --" >> $log
    sleep 3
done
