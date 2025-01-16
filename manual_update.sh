#!/bin/bash

# set working directory to ./src
cd ./src
ARG=$1

#get todays date
DATE=$(date -d $ARG '+%Y-%m-%d')

# Print message, download and modify meta data from RKI server
STARTTIME=`date +%s`
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Start update with archive data"
python build_archive_meta_manualrun.py $ARG

# Print message, create new json files for date
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python update_github_action.py"
python update_manual.py

# Print message, overwriting meta.json
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : overwriting meta.json with meta_new.json"
/bin/mv -f ../dataStore/meta/meta_new.json ../dataStore/meta/meta.json

# download static 7zip
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : download static 7zip"
cd ../
VERSION7ZIP="2409"
./get7Zip.sh ${VERSION7ZIP}

YEAR=${ARG:0:4}
# start compress RKI_COVID19_$DATE.csv
if [ ! -f "./data/$YEAR/RKI_COVID19_$DATE.csv.xz" ]; then
  cd ./data/$YEAR
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  SIZE1=$(stat -c%s RKI_COVID19_$DATE.csv)
  echo -n "$DATE2 : compressing RKI_COVID19_$DATE.csv ($SIZE1 bytes); "
  mv "RKI_COVID19_$DATE.csv" "temp.csv"
  sort -t',' -n -k 1,1 -k 2,2 -k 3,3 -k 10,10 temp.csv > "RKI_COVID19_$DATE.csv"
  rm -f temp.csv
  ../../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "RKI_COVID19_$DATE.csv.xz" "RKI_COVID19_$DATE.csv"
  SIZE2=$(stat -c%s RKI_COVID19_$DATE.csv.xz)
  QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
  echo "New Size: $SIZE2 = $QUOTE %"
fi

# compress json files

rm -f dataStore/**/*.xz
for file in `find dataStore/ -name "*.json"  ! -name "meta.json" -type f`;
  do
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    SIZE1=$(stat -c%s "$file")
    echo -n "$DATE2 : compressing $file ($SIZE1 bytes); "
    ./7zzs a -txz -mmt4 -mx=5 -sdel -stl -bso0 -bsp0 "./$file.xz" "./$file"
    SIZE2=$(stat -c%s "$file.xz")
    QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
    echo "New Size: $SIZE2 = $QUOTE %"
  done
rm -f 7zzs

git add ':/*.csv'
git add ':/*.json'
git add ':/*.xz'
git status -s
git commit -m "update $ARG"
git tag -a "v1.9.$(date -d $ARG '+%Y%m%d')" -m "v1.9.$(date -d $ARG '+%Y%m%d') release"
git push
git push origin tag "v1.9.$(date -d $ARG '+%Y%m%d')"
gh release create "v1.9.$(date -d $ARG '+%Y%m%d')" --generate-notes

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
ENDTIME=`date +%s`
TOTALSEC=`expr $ENDTIME - $STARTTIME`
TIME=`date -d@$TOTALSEC -u +%H:%M:%S`
echo "*************************************************************************"
echo "$DATE2 : Update finished. Total execution time $TIME . *"
echo "*************************************************************************"
