
在${HIVE_HOME}中创建文件夹auxlib , 然后将自定义jar文件放入该文件夹中。

delete jar /tmp/5e111f51-87c6-4f19-b9b4-b3e132cec7ad_resources/hive-functions-1.0-SNAPSHOT.jar;
list jars;

add jar hdfs:///libs/common/hive-functions-1.0-SNAPSHOT.jar;
list jars;

drop temporary function if exists CreateAppId;
create temporary function CreateAppId as 'cn.whaley.bi.logsys.hive.functions.CreateAppId';
describe function extended CreateAppId;

select app_id,org_code,product_code,app_code
    ,CreateAppId(org_code,product_code,app_code) as createdId
from metadata.app_id_info;

select CreateAppId('whaley','whaleyvr','main') as createdId
from sys.dual;


====Issue====
1. jar包更新需要重启会话,意味着hiveserver2需要重启或HiveCli进程需要退出重启
