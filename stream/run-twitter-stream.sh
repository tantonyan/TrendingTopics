#!/bin/bash

topic="twitter-all-json"

log="stream.log"

cd ~/stream
echo `date` >> $log
python stream_twitter.py $topic >> $log 2>&1
echo "\n-- -- -- -- -- --\n" >> $log
