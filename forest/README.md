####项目介绍
* 项目作用
1.将包含在一条日志里的十个子日志解析出来
2.md5校验
3.增加topicId,logID


运行方式：常驻进程
运行机器：app-1,2,3
程序入口：
cn.whaley.bi.logsys.forest.MainObj

程序参数示例：
MsgProcExecutor \
--f MsgBatchManager.xml,settings.properties \
--c prop.KafkaMsgSource.topicRegex=^log-raw-boikgpokn78sb95ktmsc1bnk.*$ \
--c prop.kafka-consumer.group.id=forest-dist-medusa



最新forest数据源：
/log/nginx/rawlog/20170724/13 改为：/data_warehouse/ods_origin.db/log_raw/key_day=20170724/key_hour=16