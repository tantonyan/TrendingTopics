#!/bin/bash

jar="trending_rt_topology.jar"

if [ $# -gt 0 ]; then
  jar=$1
fi

pyleus submit -n 172.31.25.203 $jar
