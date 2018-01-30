#!/bin/bash

#source ~/.bash_profile


#set -x

Params=($@)
MainClass=${Params[0]}
Length=${#Params[@]}
Args=${Params[@]:1:Length-1}

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh

load_properties ../conf/spark_log2parquet.properties

#params: $1 className, $2 propName
getSparkProp(){
    className=$1
    propName=$2

    defaultPropKey=${propName}
    defaultPropKey=${defaultPropKey//./_}
    defaultPropKey=${defaultPropKey//-/_}
    #echo "defaultPropValue=\$${defaultPropKey}"
    eval "defaultPropValue=\$${defaultPropKey}"

    propKey="${className}_${propName}"
    propKey=${propKey//./_}
    propKey=${propKey//-/_}
    eval "propValue=\$${propKey}"

    if [ -z "$propValue" ]; then
        echo "$defaultPropValue"
    else
        echo "$propValue"
    fi
}


spark_home=${spark_home:-$SPARK_HOME}
spark_master=${spark_master}
spark_mainJar="../lib/${spark_mainJarName}"
spark_driver_memory=$(getSparkProp $MainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $MainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $MainClass "spark.cores.max")
spark_shuffle_service_enabled=$(getSparkProp $MainClass "spark.shuffle.service.enabled")
spark_dynamicAllocation_enabled=$(getSparkProp $MainClass "spark.dynamicAllocation.enabled")
spark_dynamicAllocation_minExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.minExecutors")
spark_dynamicAllocation_maxExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.maxExecutors")
spark_dynamicAllocation_initialExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.initialExecutors")
spark_default_parallelism=$(getSparkProp $MainClass "spark.default.parallelism")
spark_yarn_queue=$(getSparkProp $MainClass "spark.yarn.queue")

export CLASSPATH=.:${CLASSPATH}:${pwd}/../conf

dependenceDir=/data/apps/azkaban/log2parquet

for file in ../conf/*
do
	if [ -n "$resFiles" ]; then
		resFiles="$resFiles,$file"
	else
		resFiles="$file"
    fi
done
for file in ${dependenceDir}/lib/*.jar
do
    if [[ "$file" == *${spark_mainJarName} ]]; then
        echo "skip $file"
    else
        if [ -n "$jarFiles" ]; then
            jarFiles="$jarFiles,$file"
        else
            jarFiles="$file"
        fi
    fi
done


for file in ../lib/*.jar
do
    if [[ "$file" == *${spark_mainJarName} ]]; then
        echo "skip $file"
    else
        if [ -n "$jarFiles" ]; then
            jarFiles="$jarFiles,$file"
        else
            jarFiles="$file"
        fi
    fi
done




#if [ -f "/data/apps/azkaban/share/libs/phoenix-4.10.0-HBase-1.2-thin-client-without-hadoop.jar" ]; then
#    jarFiles="$jarFiles,/data/apps/azkaban/share/libs/phoenix-4.10.0-HBase-1.2-thin-client-without-hadoop.jar"
#fi

ts=`date +%Y%m%d-%H%M%S`
set -x
$spark_home/bin/spark-submit -v \
--name guohao_metadata2ods_${app_name:-$MainClass}_$ts \
--master ${spark_master} \
--executor-memory $spark_executor_memory \
--driver-memory $spark_driver_memory \
--files $resFiles \
--jars $jarFiles \
--conf spark.cores.max=${spark_cores_max}  \
--conf spark.shuffle.service.enabled=${spark_shuffle_service_enabled} \
--conf spark.dynamicAllocation.enabled=${spark_dynamicAllocation_enabled}  \
--conf spark.dynamicAllocation.minExecutors=${spark_dynamicAllocation_minExecutors} \
--conf spark.dynamicAllocation.maxExecutors=${spark_dynamicAllocation_maxExecutors} \
--conf spark.dynamicAllocation.initialExecutors=${spark_dynamicAllocation_initialExecutors} \
--conf spark.default.parallelism=${spark_default_parallelism} \
--conf spark.yarn.queue=${spark_yarn_queue} \
--class "$MainClass" $spark_mainJar $Args