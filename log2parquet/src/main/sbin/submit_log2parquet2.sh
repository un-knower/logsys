#!/bin/bash

cd `dirname $0`
pwd=`pwd`
source ./envFn.sh
load_properties ../conf/spark_log2parquet.properties
load_args $*

ARGS=`getopt -o m:t:d:h --long mainClass:,taskFlag:,startDate:,startHour: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -m|--mainClass)
            mainClass=$2;
            shift 2;;
        -t|--taskFlag)
            taskFlag=$2;
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

startDate=`date -d "${startDate} ${startHour} -1 hour" +"%Y%m%d"`
startHour=`date -d "${startDate} ${startHour} -1 hour" +"%H"`
inputPath=/data_warehouse/ods_origin.db/log_origin/key_day=${startDate}/key_hour=${startHour}


echo "startDate is ${startDate}"
echo "startHour is ${startHour}"
echo "taskFlag is ${taskFlag}"
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
spark_master=${spark_master}
spark_mainJar="../lib/${spark_mainJarName}"
spark_driver_memory=$(getSparkProp $mainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $mainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $mainClass "spark.cores.max")
spark_shuffle_service_enabled=$(getSparkProp $mainClass "spark.shuffle.service.enabled")
spark_dynamicAllocation_enabled=$(getSparkProp $mainClass "spark.dynamicAllocation.enabled")
spark_dynamicAllocation_minExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.minExecutors")
spark_dynamicAllocation_maxExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.maxExecutors")
spark_dynamicAllocation_initialExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.initialExecutors")
spark_default_parallelism=$(getSparkProp $mainClass "spark.default.parallelism")
spark_yarn_queue=$(getSparkProp $mainClass "spark.yarn.queue")

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



ts=`date +%Y%m%d_%H%M%S`
set -x
$spark_home/bin/spark-submit -v \
--name ${app_name:-$MainClass}_$ts \
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
--class $mainClass $spark_mainJar  MsgProcExecutor  --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath} --c startDate=${startDate} --c startHour=${startHour} --c taskFlag=${taskFlag}