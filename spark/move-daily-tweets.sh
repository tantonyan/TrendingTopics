#!/bin/bash

if [ $# -lt 1 ]; then
   echo "Need a date as an argument. e.g 2015/09/27"
   exit
fi

date=$1
path_from="/camus/topics/twitter-json5/hourly/"$date
path_to="/tweets/twitter-json/daily/"$date

HDFS="/usr/local/hadoop/bin/hdfs"

echo `date`
echo "Moving files from $path_from to $path_to"

hours=( "00" "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" "13" "14" "15" "16" "17" "18" "19" "20" "21" "22" "23" )

$HDFS dfs -mkdir -p $path_to
for h in "${hours[@]}"
do
    path=$path_from"/"$h
    $HDFS dfs -mv $path"/*" $path_to"/"
done

