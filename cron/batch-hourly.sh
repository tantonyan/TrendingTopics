#!/bin/bash

log="/home/ubuntu/TrendingTopics/cron/logs/batch-hourly.log"
workdir="/home/ubuntu/TrendingTopics/spark"
cd $workdir

# the script is a cron job set to start at the beginning (02 minutes) of each hour
# run the hourly spark script for the past 3 hours (3 hour slots)

echo `date` >> $log

path_main="/camus/topics/twitter-all-json/hourly/"

path0=`date --date="1 hours ago" +%Y/%m/%d/%H`
path1=`date --date="2 hours ago" +%Y/%m/%d/%H`
path2=`date --date="3 hours ago" +%Y/%m/%d/%H`

ss="/usr/local/spark/bin/spark-submit"
arg_master="--master spark://172.31.20.120:7077"
arg_packages="--packages TargetHolding/pyspark-cassandra:0.1.5"
arg_conf="--conf spark.cassandra.connection.host=172.31.46.91"
script="hourly-run.py"

subpaths=($path0 $path1 $path2)
for p in "${subpaths[@]}"
do
    path=$path_main$p
    $ss $arg_master $arg_packages $arg_conf $script $path >> $log
done
echo "-- -- -- -- -- --" >> $log
#/usr/local/spark/bin/spark-submit --master spark://172.31.20.120:7077 --packages TargetHolding/pyspark-cassandra:0.1.5 --conf spark.cassandra.connection.host=172.31.46.91 hourly-run.py $path
