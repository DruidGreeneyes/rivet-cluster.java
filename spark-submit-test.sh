#!/bin/bash
if [[ $RIVET_LAST_PACKAGE_TIME && ${RIVET_LAST_PACKAGE_TIME-_} ]]; then
  last=$RIVET_LAST_PACKAGE_TIME
else
  echo "no previous package time data!"
  last=$(date +%s)
fi
  
opts=""
if [ -f submit.conf ]; then
  while read line
  do
   opts="$opts $line"
  done < submit.conf
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

export RIVET_LAST_PACKAGE_TIME=$last

export HADOOP_CONF_DIR=/home/josh/.hadoop/etc/hadoop
echo "press enter to run spark-submit with opts=$opts"
read -s
spark-submit $opts target/rivet.java-0.0.1-SNAPSHOT.jar $@
