#!/bin/bash
set -e

[[ $1 ]] || echo "No main class supplied."
[[ $2 ]] || echo "No input path supplied."
CLASS="$1"
IN="$2"
shift; shift

NAMENODE=54.153.23.91
PORT=22

gradle jar
scp -i keys/vm1-hadoop -P $PORT build/libs/assg1.jar "hadoop@$NAMENODE:"
ssh -i keys/vm1-hadoop -p $PORT "hadoop@$NAMENODE" "./hadoop-run-job.sh assg1.jar $CLASS $IN $@"
