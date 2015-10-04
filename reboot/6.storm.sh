#!/bin/bash

sudo $STORM_HOME/bin/storm nimbus &

sleep 20

sudo $STORM_HOME/bin/storm ui &
