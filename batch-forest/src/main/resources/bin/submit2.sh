#!/bin/bash

cd `dirname $0`
pwd=`pwd`


source ./envFn.sh
load_properties ../conf/spark.properties
load_args $*

ARGS=`getopt -o m:d:h --long mainClass:,startDate:,startHour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -m|--mainClass)
            mainClass=$2;
            shift 2;;
		-d|--startDate)
            startDate=$2;
            shift 2;;
        -h|--startHour)
            startHour=$2;
            shift 2;;
        --)
            shift;
            break;;

        *)
            exit 1
            ;;
    esac
done

inputPath="/data_warehouse/ods_origin.db/log_raw"

key_day=$startDate
key_hour=23




echo "key_day is ${key_day}"
echo "key_hour is ${key_hour}"
echo "inputPath is ${inputPath}"
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
spark_mainJar="../lib/${spark_mainJarName}"
spark_driver_memory=$(getSparkProp $mainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $mainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $mainClass "spark.cores.max")
spark_master=$(getSparkProp $mainClass "spark.master")
spark_executor_cores=$(getSparkProp $mainClass "spark.executor.cores")
spark_shuffle_service_enabled=$(getSparkProp $mainClass "spark.shuffle.service.enabled")
spark_dynamicAllocation_enabled=$(getSparkProp $mainClass "spark.dynamicAllocation.enabled")
spark_dynamicAllocation_minExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.minExecutors")
spark_dynamicAllocation_maxExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.maxExecutors")
spark_dynamicAllocation_initialExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.initialExecutors")
spark_default_parallelism=$(getSparkProp $mainClass "spark.default.parallelism")
spark_yarn_queue=$(getSparkProp $mainClass "spark.yarn.queue")

dependenceDir=/data/apps/azkaban/forest_batch


for file in ../conf/*
do
	if [ -n "$res_files" ]; then
		res_files="$res_files,$file"
	else
		res_files="$file"
    fi
done

for file in ${dependenceDir}/lib/*.jar
do
	if [[ "$file" == *${spark_mainJarName} ]]; then
		echo "skip $file"
	else
		if [ -n "$jar_files" ]; then
			jar_files="$jar_files,$file"
		else
			jar_files="$file"
		fi
	fi
done


for i in {1..24}
do
 echo $key_day$key_hour
 set -x
 ts=`date +%Y%m%d_%H%M%S`
 ${spark_home}/bin/spark-submit -v \
  --name ${app_name:-$mainClass}_$ts \
  --master ${spark_master} \
  --executor-memory ${spark_executor_memory} \
  --driver-memory ${spark_driver_memory}   \
  --jars ${jar_files} \
  --files ${res_files} \
  --conf spark.shuffle.service.enabled=${spark_shuffle_service_enabled} \
  --conf spark.dynamicAllocation.enabled=${spark_dynamicAllocation_enabled}  \
  --conf spark.dynamicAllocation.minExecutors=${spark_dynamicAllocation_minExecutors} \
  --conf spark.dynamicAllocation.maxExecutors=${spark_dynamicAllocation_maxExecutors} \
  --conf spark.dynamicAllocation.initialExecutors=${spark_dynamicAllocation_initialExecutors} \
  --conf spark.default.parallelism=${spark_default_parallelism} \
  --conf spark.yarn.queue=${spark_yarn_queue} \
  --conf spark.executor.cores=${spark_executor_cores} \
  --class $mainClass ${spark_mainJar} ${inputPath} ${key_day} ${key_hour}


 time=`date -d "$key_day $key_hour -1 hour" +"%Y%m%d%H"`
 key_day=${time:0:8}
 key_hour=${time:8:2}
done


