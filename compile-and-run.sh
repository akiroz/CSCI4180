#!/bin/bash
set -e

[[ $1 ]] || echo "No main class supplied."
[[ $2 ]] || echo "No input path supplied."
CLASS="$1"
IN="$2"
shift; shift

#NAMENODE=54.215.247.186
#PORT=22
NAMENODE=137.189.89.214
PORT=12231

export SRC=asgn2

[[ $SKIP ]] || gradle jar
[[ $SKIP ]] || scp -i keys/vm1-hadoop -P $PORT build/libs/CSCI4180.jar "hadoop@$NAMENODE:"
ssh -i keys/vm1-hadoop -p $PORT "hadoop@$NAMENODE" "export DEBUG=1; ./hadoop-run-job.sh CSCI4180.jar $CLASS $IN $@"

tput bel
