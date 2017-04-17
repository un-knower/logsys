####################################################################################
#
# example:
#   ./createAppId.sh whaley medusa main
#
####################################################################################
classpath="./"
for file in ./*.jar
do
    classpath="$classpath:$file"
done
java -classpath $classpath cn.whaley.bi.logsys.common.AppIdCreator $@
