--在hive-cli中执行:

DROP FUNCTION IF EXISTS sys.CreateAppId;
CREATE FUNCTION sys.CreateAppId AS 'cn.whaley.bi.logsys.hive.functions.CreateAppId'
USING JAR 'hdfs:///libs/common/hive-functions-1.0-SNAPSHOT.jar';

DROP FUNCTION IF EXISTS sys.JsonArrayStrExplode;
CREATE FUNCTION sys.JsonArrayStrExplode AS 'cn.whaley.bi.logsys.hive.functions.JsonArrayStrExplode'
USING JAR 'hdfs:///libs/common/hive-functions-1.0-SNAPSHOT.jar';

show functions like 'sys.*';

--重启hiveserver2,以便函数生效

