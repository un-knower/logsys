
在${HIVE_HOME}中创建文件夹auxlib , 然后将自定义jar文件放入该文件夹中。

delete jar /tmp/5e111f51-87c6-4f19-b9b4-b3e132cec7ad_resources/hive-functions-1.0-SNAPSHOT.jar;
list jars;

add jar hdfs:///libs/common/hive-functions-1.0-SNAPSHOT.jar;
list jars;

DROP TEMPORARY FUNCTION IF EXISTS CreateAppId;
CREATE TEMPORARY FUNCTION CreateAppId AS 'cn.whaley.bi.logsys.hive.functions.CreateAppId'
USING JAR 'hdfs:///libs/common/hive-functions-1.0-SNAPSHOT.jar';

show functions like 'CreateAppId';
describe function extended CreateAppId;
select CreateAppId('whaley','whaleyvr','main') as createdId;

DROP TEMPORARY FUNCTION IF EXISTS JsonArrayStrExplode;
CREATE TEMPORARY FUNCTION JsonArrayStrExplode AS 'cn.whaley.bi.logsys.hive.functions.JsonArrayStrExplode'
USING JAR 'hdfs:///libs/common/hive-functions-1.0-SNAPSHOT.jar';
select JsonArrayStrExplode('[{"rowId":"1"},{"rowId":"2"}]');
select s.*,t.row_num,t.row_value
from sys.dual s lateral view JsonArrayStrExplode('[{"rowId":"1"},{"rowId":"2"}]') t as row_num,row_value;


====自动加载====
hive-cli工具: 可在.hiverc中添加初始化脚本
服务级别(如hiveserver2)jar包需要添加到hive-site.xml的hive.aux.jars.path配置项中

====Issue====
1. jar包更新需要重启会话,意味着hiveserver2需要重启或HiveCli进程需要退出重启


