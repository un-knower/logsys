####项目介绍
* 项目作用
1.将ods按小时分割的数据，做数据清洗并转化为parquet文件
2.生成供`元数据模块`使用的基础信息


####azkaban部署
项目名称：ods_view_log2parquet

####测试运行环境
机器：spark@bigdata-appsvr-130-5
测试运行目录： /app/ods_view_log2parquet

####TODO:
1. 发现每次都要mvn clean；才能保证settings.properties不会打入jar包，但是不打入jar包，confManager加载不到资源
报错信息：
can not load resource settings.properties
can not load resource MsgBatchManagerV3.xml

解决方式：yarn-cluster;进一步调研yarn-client模式的CLASSPATH方式加载

2. 性能改进
a. 有些不需要返回RDD的操作
.map 改成.foreach

3. task表本身信息没有填充

4. 只拿paruqet的一个文件生成metadata.logfile_field_desc表，需要测试，是否一个parquet文件的schema就可以代表所有parquet文件的schema

5. taskFlag以程序传入方式

6. crash日志还需要校验md5吗？yes

7. realIp处理器

8. 规则检查
* 字段名不允许出现"."和"-"
* eventId的value作为输出路径的值的时候，将.或-变为_;eventId的value在jsonObject不用变

9. medusa 2.x 日志
需要在下面代码前判断是否为medusa 2.x日志
 val pathRdd = MetaDataUtils.parseLogStrRddPath(rdd_original)
 
10. 连凯提出的helios-whaleyvip-activity异常日志，需要在
val pathRdd = metaDataUtils.parseLogStrRddPath(rdd_original)
过程中处理

11. Json2ParquetUtil.saveAsParquet 后期稳定后，删除临时文件代码修改

12. 输入路径正则匹配测试
 使用azkaban调度传入day和hour，会运行所有的appid的当前hour
 /data_warehouse/ods_origin.db/log_origin/key_appId=*/key_day=${yyyymmdd}/key_hour=${HH}
 
13. 分批put，post数据 

14. # 分隔符改为其他字符


