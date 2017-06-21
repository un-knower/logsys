####项目介绍
* 项目作用
1.用来将ods按小时分割的数据，以logType分割并转化为parquet文件
2.生成供`元数据模块`使用的基础信息

* 项目技术依赖
1.使用phoenix客户端读取和写入元数据基础信息
2.使用spark完成数据源读取和parquet文件转换

####测试运行环境

####phoenix DDL and DML
* 创建Table
CREATE TABLE IF NOT EXISTS METADATA.APPLOG_SPECIAL_FIELD_DESC_TEST(
id BIGINT not null primary key,
tabNameReg VARCHAR(100)
)


* 创建sequence
CREATE SEQUENCE METADATA.APPLOG_SPECIAL_FIELD_DESC_SEQ;

* 插入记录
UPSERT INTO METADATA.APPLOG_SPECIAL_FIELD_DESC_TEST VALUES(NEXT VALUE FOR METADATA.APPLOG_SPECIAL_FIELD_DESC_SEQ, 'foo');

* 查询表
select * from METADATA.APPLOG_SPECIAL_FIELD_DESC_TEST;

 

####phoenix命令行使用：
1.连接phoenix
hadoop@bigdata-appsvr-130-5
cd /opt/phoenix/bin
./sqlline.py bigdata-cmpt-128-1:2181
s
2.命令行例子
!describe METADATA.APPLOG_SPECIAL_FIELD_DESC


####业务逻辑
* 程序入口输入参数
appId，day，hour

* ods origin路径生成规则
/data_warehouse/ods_origin.db/log_origin/key_appId=${appId}/key_day=${yyyymmdd}/key_hour=${HH}

* ods view路径生成规则
路径模版：/data_warehouse/ods_view.db/t_log/productCode=../appCode=../logType=../eventId=../key_day=${yyyymmdd}/key_hour=${HH}
productCode,appCode通过ap pi
select * from metadata.app_metadata_idinfo where app_id='xxx'

logType规则：
 logType非start_end,使用eventId。如果没有eventId,使用默认值【default】
 logType为start_end,使用actionId作为eventId的值


hadoop fs -du -h /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=12


####疑问
1.需要做md5校验吗？no
2.分post请求，get请求？yes
3.realLogType处理逻辑? yes
val realLogType = if(EVENT == logType){
            getStringValue(log,EVENT_ID)
          }else if(START_END == logType){
            getStringValue(log,ACTION_ID)
          }else logType
4.除了logBody，还有哪些字段需要加入转parquet的json? all of them
5.remoteIp和forwardedIp对应？find svr_forwarded_for,svr_remote_addr in log
6.参考代码MedusaLog2Parquet?yes

####信息同步
2.x 代码逻辑参考forest项目GenericActionLogGetProcessor类parseMedusa20Log方法。


在原有log2parquet需要做一条日志解析为多条日志的行为，现有log2parquet无需此操作。

