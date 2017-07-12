

####TODO----------------------------

1. 对于设置cronjob，对于输入的源数据，最终生成时间无法准确把握，需要假设一个最大的时间
 2017-07-12 10:21 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendo8dc5mlsr/key_day=20170712/key_hour=09/boikgpokn78sb95kjhfrendo8dc5mlsr_2017071209_raw_8_421773485.json.gz

2. 性能改进
a. 有些不需要返回RDD的操作
.map 改成.foreach

3. task表本身信息没有填充

4. 只拿paruqet的一个文件生成metadata.logfile_field_desc表，需要测试，是否一个parquet文件的schema就可以代表所有parquet文件的schema

5. taskFlag以程序传入方式

6. crash日志还需要校验md5吗？yes

8. 规则检查
* 字段名不允许出现"."和"-"
* eventId的value作为输出路径的值的时候，将.或-变为_;eventId的value在jsonObject不用变

9. medusa 2.x 日志[连凯TODO]
需要在下面代码前判断是否为medusa 2.x日志
 val pathRdd = MetaDataUtils.parseLogStrRddPath(rdd_original)

11. Json2ParquetUtil.saveAsParquet 后期稳定后，删除临时文件代码修改

17. pathRdd需要加的逻辑
连凯提出的helios-whaleyvip-activity异常日志，需要在
val pathRdd = metaDataUtils.parseLogStrRddPath(rdd_original)
if(jsonObject.containsKey("logType")&&jsonObject.getString("logType").equalsIgnoreCase("helios-whaleyvip-activity")){
        jsonObject.put("logType","event")
      }

12. 输入路径正则匹配测试
 使用azkaban调度传入day和hour，会运行所有的appid的当前hour
 /data_warehouse/ods_origin.db/log_origin/key_appId=*/key_day=${yyyymmdd}/key_hour=${HH}
 还是分appId运行，保证不同appId能够尽可能快的被查询
 
 

####DONE----------------------------
1. 发现每次都要mvn clean；才能保证settings.properties不会打入jar包
  但是不打入jar包，以yarn-client模式运行，confManager加载不到资源
报错信息：
can not load resource settings.properties
can not load resource MsgBatchManagerV3.xml

解决方式：yarn-cluster;进一步调研yarn-client模式的CLASSPATH方式加载

7. realIp处理器
  private val REMOTE_IP = "remoteIp"
  private val FORWARDED_IP = "forwardedIp"
  svr_forwarded_for
  svr_remote_addr

13. 分批put，post数据,发送请求到冯进的phoenix http server

14. #分隔符改为其他字符
未改动
hadoop fs -rm -r /log/default/parquet/aa/bb#cc 会删除的,并且只在/log/default/parquet/临时目录操作

15. 看到yarn-cluster执行了两次
    因为yarn本身有重试机制，查看看yarn ui界面，观察一个Application的Attempt ID个数，Attempt所在机器也是yarn-cluster模式的spark driver所在的机器。

16. 期望不要等待返回结果
http://bigdata-appsvr-130-5:8084/metadata/processTask/AAABXSxrUCwK4Aaq1wAAAAA/111
解决方式：调整timeout方式


