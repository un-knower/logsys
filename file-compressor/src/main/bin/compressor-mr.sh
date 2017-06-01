
cd `dirname $0`
pwd=`pwd`


for file in $pwd/../conf
do
    if [ -z $confFiles ]; then
        confFiles="$file"
    else
        confFiles="$file:$confFiles"
    fi
done

export HADOOP_CLASSPATH="$pwd/../conf"
for file in $pwd/../lib/*.jar
do
    export HADOOP_CLASSPATH="$file:$HADOOP_CLASSPATH"
    if [ -z $libjars ]; then
        libjars="$file"
    else
        libjars="$file,$libjars"
    fi
done

hadoop jar $pwd/../lib/file-compressor-1.0-SNAPSHOT.jar cn.whaley.bi.logsys.filecompressor.MapReduceClient \
  -files $confFiles -libjars $libjars $@

