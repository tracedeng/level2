#!/bin/sh
minite=`date "+%Y%m%d_%H%M"`
path="/root/backup/"$minite
path2="/media/backup/"$minite
path3="/media/biiyoo/level2/test/backup/"$minite

mongodump -h localhost -d lv2 -o $path &>/dev/null
mongodump -h localhost -d lv2 -o $path2 &>/dev/null
mongodump -h localhost -d lv2 -o $path3 &>/dev/null
