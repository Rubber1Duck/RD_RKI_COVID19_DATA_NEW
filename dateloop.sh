#!/bin/bash
STARTMAIN=`date +%s`
startdate=$(date -I -d "$1") || exit -1
enddate=$(date -I -d "$2")     || exit -1

d="$startdate"
while [ "$d" != "$enddate" ]; do 
  ./manual_update.sh $d
  d=$(date -I -d "$d + 1 day")
done
./manual_update.sh $d

ENDMAIN=`date +%s`
TOTALMAIN=`expr $ENDMAIN - $STARTMAIN`
TIMEMAIN=`date -d@$TOTALMAIN -u +%H:%M:%S`
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "*****************************************************************************************************"
#     2025-01-15T12:30:00 : Update from 2020-04-08 to 2020-12-31 finished. Total execution time 00:00:00 .*
echo "$DATE2 : Update from $startdate to $enddate finished. Total execution time $TIME .*"
echo "*****************************************************************************************************"
