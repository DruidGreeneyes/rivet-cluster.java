#!/bin/bash
if [ -f test/last-package-time ]; then
  while read line
  do
    last=line
  done < test/last-package-time
else
  echo "no previous package time data!"
  last=0
fi
  
opts=""
if [ -f conf/submit.conf ]; then
  while read line
  do
    if [[! "$line" =~ ^# ]]; then
     opts="$opts $line"
    fi
  done < conf/submit.conf
fi

latest=$(find . -type f | xargs ls -tr | tail -n 1)
latest=$(stat --printf=%X $latest)

if (( $last < $latest )); then
  echo 'source has been modified since last run -- press enter to re-package'
  read -s
  echo 'packaging...'
  mvn package > package.log.txt
  echo 'finished packaging'
  last=$(date +%s)
fi

echo $last > test/last-package-time

echo "press enter to run spark-submit with opts=$opts"
read -s
spark-submit $opts target/rivet.java-0.0.1-SNAPSHOT.jar $@
