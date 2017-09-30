#!/bin/bash
set -e
[[ $1 ]] || echo "No main class supplied."
[[ $2 ]] || echo "No input path supplied."
CLASS="$1"
IN="$2"
shift; shift
gradle jar
scp -i keys/vm1-hadoop -P 12231 build/libs/assg1.jar hadoop@137.189.89.214:
ssh -i keys/vm1-hadoop -p 12231 hadoop@137.189.89.214 "./hadoop-run-job.sh assg1.jar $CLASS $IN $@"
