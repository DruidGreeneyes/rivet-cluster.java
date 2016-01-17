#!/bin/bash
###############
# Riveting. First arg is path to the source
##############
echo "Rivet, Rivet"

source ./classpath.sh
cd ..
mvn -DskipTests clean package

#echo "$(cygpath -pw "$CLASSPATH")"
java -Xmx10g -Dfile.encoding=UTF-8 -classpath "$(cygpath -pw "$CLASSPATH")" testing.Program  $1

