#####################################################
#  --daemon:  为true时用nohup启动
#  --startDate: 数据产生起始日期
#  --endDate: 数据产生结束日期
#  --offset: 数据产生起始日期偏移值,如-1为起始日期的T-1日
#  --split: 数据分片数量
#  --codec: 压缩格式编码器
#  --mapreduce_map_memory_mb:  map内存设置
#
# 以默认参数处理当日数据
# ./sbin/compress_api_nginx_log_moretv_recommend.sh
#
# 指定参数处理特定日志数据
# ./sbin/compress_api_nginx_log_moretv_recommend.sh --startDate=20170524 --endDate=20170524 --split=10 --codec=Lz4Codec --mapreduce_map_memory_mb=2048
#

cd `dirname $0`
pwd=`pwd`

source $pwd/../bin/envFn.sh
load_args $@

preDate=`date -d "-1 days " +%Y%m%d`
startDate=${startDate:-$preDate}
endDate=${endDate:-$preDate}
currDate=$startDate

if [ -n $offset ]; then
    startDate=`date -d "$offset days "$startDate +%Y%m%d`
fi

echo `date` "startDate is $startDate, endDate is $endDate"
while [[ $currDate -le $endDate ]]
do
    echo `date` "currDate is $currDate"

    srcPath=/log/api_nginx_log_moretv_recommend/$currDate/*
    outPath=/log/api_nginx_log_moretv_recommend_split/$currDate
    split=${split:-10}
    codec=${codec:-Lz4Codec}
    mapreduce_map_memory_mb=${mapreduce_map_memory_mb:-2048}
    mapreduce_job_name="compress_${currDate}_api_nginx_log_moretv_recommend"

    set -x
    if [ "$daemon" == "true"  ]; then
        nohup sh $pwd/../bin/compressor-mr.sh \
            -D mapreduce.map.memory.mb=${mapreduce_map_memory_mb} \
            -D mapreduce.job.name=${mapreduce_job_name} \
            -D mapreduce.job.queuename=bi \
            --cmd=compress \
            --srcPath=$srcPath \
            --outPath=$outPath \
            --split=$split \
            --codec=Lz4Codec \
         >> ${pwd}/../logs/api_nginx_log_moretv_recommend.log 2>&1 &
    else
        sh $pwd/../bin/compressor-mr.sh \
            -D mapreduce.map.memory.mb=${mapreduce_map_memory_mb} \
            -D mapreduce.job.name=${mapreduce_job_name} \
            -D mapreduce.job.queuename=bi \
            --cmd=compress \
            --srcPath=$srcPath \
            --outPath=$outPath \
            --split=$split \
            --codec=Lz4Codec
    fi
    set +x

    currDate=`date -d "1 days "$currDate +%Y%m%d`
    echo `date` "set currDate to $currDate"

done