#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`
echo "${pwd}"
service_bin_home="../sbin"

usage() { echo "Usage: $0 [-p <string:path> -d <string:day> -j <string:isJsonDirDelete> -t <string:isTmpDirDelete> -f <string:taskFlag>]" 1>&2; exit 1; }

while getopts ":p:d:j:t:f:" o; do
    case "${o}" in
        p)
            p=${OPTARG}
            ;;
        d)
            d=${OPTARG}
            ;;
        j)
            j=${OPTARG}
            ;;
        t)
            t=${OPTARG}
            ;;
        f)
            f=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${p}" ] || [ -z "${d}" ] || [ -z "${j}" ] || [ -z "${t}" ] || [ -z "${f}" ]; then
    usage
fi


#日期减去一天，获得新的day
newDate=`date -d "${d} -1 day" +"%Y%m%d"`
echo "newDate:${newDate}"
inputPath=${p}/key_day=${newDate}
echo "inputPath:${inputPath},newDate:${newDate},isJsonDirDelete:${j},isTmpDirDelete:${t},taskFlag:${f}"
sh ${service_bin_home}/submit.sh cn.whaley.bi.logsys.log2parquet.MainObj MsgProcExecutor --f MsgBatchManagerV3.xml,settings.properties --c inputPath=${inputPath} --c isJsonDirDelete=${j} --c isTmpDirDelete=${t} --c taskFlag=${f}
