#!/bin/bash
set -e
JAR="$1"
CLASS="$2"
IN="$3"
OUT="/$2-out"
export HADOOP_HOME=/home/hadoop/hadoop-2.7.3
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
shift; shift; shift
hadoop fs -rm -r $OUT || true
hadoop jar "$JAR" "$CLASS" "$IN" "$OUT" "$@"
hadoop fs -cat "$OUT/part-r-00000"
