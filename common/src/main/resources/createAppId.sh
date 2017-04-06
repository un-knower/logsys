####################################################################################
#
# example:
#   ./createAppId.sh whaley medusa main
#
####################################################################################

SCALA_HOME=${SCALA_HOME:-/usr/local/scala-2.11.5}
classpath="${SCALA_HOME}/lib/scala-library.jar"
for file in ./common*.jar
do
    classpath="$classpath:$file"
done
for file in ../common*.jar
do
    classpath="$classpath:$file"
done
java -classpath $classpath cn.whaley.bi.logsys.common.AppIdCreator $@