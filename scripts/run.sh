#!/bin/bash
###############
# Riveting
##############
echo "Rivet, Rivet"

source ./classpath.sh
cd ..
mvn -DskipTests clean package

#echo "$(cygpath -pw "$CLASSPATH")"
java -Xmx10g -Dfile.encoding=UTF-8 -classpath "$(cygpath -pw "$CLASSPATH")" testing.Program 

