lzo文件索引 [ LzoCodec ]:
/hadoop jar hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer   xxx.lzo

Hadoop支持以下几种压缩格式

压缩格式	UNIX工具	算      法	文件扩展名	支持多文件	可分割
DEFLATE	无	DEFLATE	.deflate	No	No
gzip	gzip	DEFLATE	.gz	No	No
zip	zip	DEFLATE	.zip	YES	YES
bzip	bzip2	bzip2	.bz2	No	YES
LZO	lzop	LZO	.lzo	No	No


为了支持多种压缩/解压缩算法，Hadoop引入了编码/解码器，如下表所示
压缩格式	对应的编码/解码器
DEFLATE	org.apache.hadoop.io.compress.DefaultCodec
gzip	org.apache.hadoop.io.compress.GzipCodec
bzip	org.apache.hadoop.io.compress.BZipCodec
Snappy	org.apache.hadoop.io.compress.SnappyCodec



hadoop jar file-compressor-1.0-SNAPSHOT.jar cn.whaley.bi.logsys.filecompressor.Compressor  \
  --cmd=decompress  \
  --srcPath=/log/api_nginx_log_moretv_recommend/20170424/data_databack_Moretv_recommended_nginx01_log__data_logs_nginx---rec.tvmore.com.cn.access.log-20170424.gz

export HADOOP_CLIENT_OPTS="-Xms4096m -Xmx4096m"
hadoop jar file-compressor-1.0-SNAPSHOT.jar \
  cn.whaley.bi.logsys.filecompressor.Compressor  \
  --cmd=compress  \
  --split=10 \
  --bufSize=10485760 \
  --codec=SnappyCodec \
  --srcPath=/log/api_nginx_log_moretv_recommend/20170424/data_databack_Moretv_recommended_nginx01_log__data_logs_nginx---rec.tvmore.com.cn.access.log-20170424.gz \
  --outPath=/tmp/compressor/data_databack_Moretv_recommended_nginx01_log__data_logs_nginx---rec.tvmore.com.cn.access.log-20170424

--SnappyCodec 2.9G 17:37:10-17:39:41 2min:30s
--BZip2Codec
--GzipCodec 1.5 G 18:29:08-18:35:26 6min
--Lz4Codec 18:39:33-18:42:15 2min:45s

hadoop fs -ls /tmp/compressor/*
hadoop fs -rm /tmp/compressor/*.bz2

hadoop fs -ls /log/api_nginx_log_moretv_recommend/20170424/data_databack_Moretv_recommended_nginx01_log__data_logs_nginx---rec.tvmore.com.cn.access.log-20170424.*.gz
hadoop fs -rm /log/api_nginx_log_moretv_recommend/20170424/data_databack_Moretv_recommended_nginx01_log__data_logs_nginx---rec.tvmore.com.cn.access.log-20170424.*.gz

ps -ef|grep compressor

jstat -gccapacity 76087
jstat -gcutil 130490 1000 100
jstat -gc 130490

---spark------
srcPath=/log/api_nginx_log_moretv_recommend/20170424/*
outPath=/tmp/compressor
numExecutor=`hadoop fs -ls $srcPath|wc -l`
split=10
yarnMaxCore=8
numCores=$split
if [ $numCores -gt  $yarnMaxCore ]; then
  numCores=$yarnMaxCore
fi
numCores=1
echo `date` "task started"
/opt/spark2/bin/spark-submit -v \
 --name file-compressor \
 --master yarn-client \
 --executor-memory 2g \
 --driver-memory 2g  \
 --num-executors $numExecutor \
 --executor-cores $numCores \
 --queue bi \
 --files log4j.properties \
 --class cn.whaley.bi.logsys.filecompressor.SparkTask file-compressor-1.0-SNAPSHOT.jar \
   --cmd=compress  \
   --split=$split \
   --bufSize=10485760 \
   --codec=Lz4Codec \
   --srcPath=$srcPath \
   --outPath=$outPath
echo `date` "task end"

--mapreduce-----
pwd=`pwd`
libjars=$pwd/fastjson-1.2.28.jar,$pwd/slf4j-log4j12-1.7.10.jar,$pwd/slf4j-api-1.7.5.jar
srcPath=/log/api_nginx_log_moretv_recommend/20170424/*
outPath=/tmp/compressor
split=10
export HADOOP_CLASSPATH="$pwd/fastjson-1.2.28.jar:$pwd/slf4j-log4j12-1.7.10.jar:$pwd/slf4j-api-1.7.5.jar"
hadoop jar file-compressor-1.0-SNAPSHOT.jar cn.whaley.bi.logsys.filecompressor.MapReduceClient  \
    -files log4j.properties \
    -libjars $libjars \
    -D mapreduce.map.memory.mb=4096 \
    --cmd=compress  \
    --split=$split \
    --bufSize=10485760 \
    --codec=Lz4Codec \
    --srcPath=$srcPath \
    --outPath=$outPath


pwd=`pwd`
libjars=$pwd/fastjson-1.2.28.jar,$pwd/slf4j-log4j12-1.7.10.jar,$pwd/slf4j-api-1.7.5.jar
srcPath=/log/api_nginx_log_moretv_recommend/20170424/*
outPath=/tmp/compressor
export HADOOP_CLASSPATH="$pwd/fastjson-1.2.28.jar:$pwd/slf4j-log4j12-1.7.10.jar:$pwd/slf4j-api-1.7.5.jar"
hadoop jar file-compressor-1.0-SNAPSHOT.jar cn.whaley.bi.logsys.filecompressor.MapReduceClient  \
    -files log4j.properties \
    -libjars $libjars \
    -D mapreduce.map.memory.mb=4096 \
    --cmd=decompress  \
    --bufSize=10485760 \
    --srcPath=$srcPath \
    --outPath=$outPath

