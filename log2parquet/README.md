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
);


* 创建sequence
CREATE SEQUENCE METADATA.APPLOG_SPECIAL_FIELD_DESC_SEQ;

* 插入记录
UPSERT INTO METADATA.APPLOG_SPECIAL_FIELD_DESC_TEST VALUES( NEXT VALUE FOR METADATA.APPLOG_SPECIAL_FIELD_DESC_SEQ, 'foo');

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
