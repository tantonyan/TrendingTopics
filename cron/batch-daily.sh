#!/bin/bash

log="/home/ubuntu/TrendingTopics/cron/batch-daily.log"
workdir="/home/ubuntu/TrendingTopics/spark"
cd $workdir

echo `date` >> $log

# run the daily spark scrip for a given date or yesterday's date
date=`date --date="1 days ago" +%Y/%m/%d`
if [ $# -gt 0 ]; then
  date=$1
fi
./move-daily-tweets.sh $date

path_main="/tweets/twitter-json/daily/"

ss="/usr/local/spark/bin/spark-submit"
arg_master="--master spark://172.31.20.120:7077"
arg_packages="--packages TargetHolding/pyspark-cassandra:0.1.5"
arg_conf="--conf spark.cassandra.connection.host=172.31.46.91"
script="hourly-run.py"

path=$path_main$date

$ss $arg_master $arg_packages $arg_conf $script $path >> $log
#/usr/local/spark/bin/spark-submit --master spark://172.31.20.120:7077 --packages TargetHolding/pyspark-cassandra:0.1.5 --conf spark.cassandra.connection.host=172.31.46.91 daily-run.py $path

echo "\n-- -- -- -- -- --\n" >> $log
