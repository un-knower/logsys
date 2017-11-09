#!/bin/bash

cd `dirname $0`
pwd=`pwd`

dependenceDir=/data/apps/azkaban/metadataManage

for file in ../conf/*
do
        if [ -n "$res_files" ]; then
                res_files="$res_files:$file"
        else
                res_files="$file"
    fi
done

for file in ${dependenceDir}/lib/*.jar
do
        if [ -n "$jar_files" ]; then
                jar_files="$jar_files:$file"
        else
                jar_files="$file"
        fi
done

mainJar=../lib/metadataManage-1.0-SNAPSHOT.jar


java -cp $jar_files:$mainJar  $*
