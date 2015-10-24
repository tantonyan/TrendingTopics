#!/usr/bin/python

import sys
import datetime

print(
    datetime.datetime.fromtimestamp(
        int(sys.argv[1])/1000.0 # we get ms from Twitter
    ).strftime('%Y-%m-%d %H:%M:%S')
)
