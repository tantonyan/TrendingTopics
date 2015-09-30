#!/bin/bash

$HADOOP_HOME/sbin/start-dfs.sh
sleep 10
$HADOOP_HOME/sbin/start-yarn.sh
sleep 10
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
