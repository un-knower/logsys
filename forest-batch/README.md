####项目介绍
* 项目作用
为保证日志处理的稳定性，采用离线批处理的方式每小时处理nginx日志。
离线处理存储在HDFS上的nginx日志 
目录例如：/data_warehouse/ods_origin.db/log_raw/key_day=20170724/key_hour=16
生成供ods_view_log2parquet使用的源数据：
例如：/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13



思路：
1. 读取/data_warehouse/ods_origin.db/log_raw/key_day=20170724/key_hour=16/目录下文件
weixinlog.moretv.log
sign_fail_boikgpokn78sb95kjhfrendoj8ilnoi7.log
取".log"前面的字符串作为输出路径的appId
答：no,

2. 原有topicAndQueue方式，是实时的处理，现在改为每个小时处理一批。在单机上处理还是使用spark分布式处理？
答：代码逻辑步骤：
 a.使用spark api读取/data_warehouse/ods_origin.db/log_raw/key_day=20170724/key_hour=16/目录下的appId特定规则的文件目录
 b.让每条msg日志经过处理器链路处理【decode、平展化、md5校验、crash日志md5校验单独处理、增加logId等必要字段】
 c.生成元组的RDD，(outPutPath,具体每条日志的信息)，复用json2ParuqetUtil的写json文件代码，输出json.gz文件
 
3. writeEntity 用来拼接_sync信息
        msgBody.put("_sync", syncInfo)
        


问题确认:
1. 需要连凯将/data_warehouse/ods_origin.db/log_raw/key_day=20170724/key_hour=16/目录下的文件更名
 例如mtvkidslog.moretv.log-2017072416-bigdata-extsvr-log1更名为为appId.log-2017072416-bigdata-extsvr-log1
 ，类似boikgpokn78sb95ktmsc1bnkfipphckl.log-2017072416-bigdata-extsvr-log7
答：只取key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l这种特征的目录文件夹下的文件做操作。
 
2. 需要处理的appId前缀都带有如下特征？
boikgpokn78sb95k
例如：../boikgpokn78sb95k7id7n8eb8dc5mlsr.log-2017072416-bigdata-extsvr-log3


# TODO
1. 初始化项目forest-batch
2. 查找、抽取处理器链路逻辑
3. 编写输出元组rdd代码，元组格式(输出路径,日志字符串)
4. 正常\异常日志输出(参考json2parquetUtil的写json文件逻辑)
5. 累加器统计各个阶段的运行情况
6. decode、平展化、md5校验、crash日志md5校验单独处理、增加logId等必要字段