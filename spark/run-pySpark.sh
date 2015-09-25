#!/bin/bash

/usr/local/spark/bin/spark-submit --master spark://172.31.20.120:7077 --packages TargetHolding/pyspark-cassandra:0.1.5 --conf spark.cassandra.connection.host=172.31.46.91 tweet-test.py
