#!/usr/bin/env bash

alias fget='python /data/tscripts/scripts/ftp.py -s get -f '
cd /app/log2parquet/lib
fget log2parquet-1.0-SNAPSHOT-michael.jar
md5sum log2parquet-1.0-SNAPSHOT-michael.jar
mv log2parquet-1.0-SNAPSHOT.jar log2parquet-1.0-SNAPSHOT.jar.old
mv log2parquet-1.0-SNAPSHOT-michael.jar log2parquet-1.0-SNAPSHOT.jar