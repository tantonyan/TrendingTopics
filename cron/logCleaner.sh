#!/bin/bash

log="logs/*log"

workdir="/usr/local/hadoop/etc/hadoop/"
oldLogsDir="oldLogs/"

cd $workdir

mv $log $oldLogsDir
