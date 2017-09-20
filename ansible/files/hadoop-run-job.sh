#!/bin/bash
set -e
JAR="$1"
CLASS="$2"
IN="$3"
OUT="/$2-out"
hadoop fs -rm -r $OUT || true
hadoop jar "$JAR" "$CLASS" "$IN" "$OUT"
hadoop fs -cat "$OUT/part-r-00000"
