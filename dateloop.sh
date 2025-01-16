#!/bin/bash

startdate=$(date -I -d "$1") || exit -1
enddate=$(date -I -d "$2")     || exit -1

d="$startdate"
while [ "$d" != "$enddate" ]; do 
  ./manual_update.sh $d
  d=$(date -I -d "$d + 1 day")
done
./manual_update.sh $d
