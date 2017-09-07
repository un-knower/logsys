1.inputPath :/data_warehouse/ods_origin.db/log_raw/key_day=20170828
2.appid 说明 hive 中 select * from metadata.app_id_info;

vr日志不处理 没有baseInfo

输出： /data_warehouse/ods_origin.db/log_origin/*/key_day=20170829/key_hour=18




一、日志校验
1)解码，nginx 收集到的日志需要处理字符转换
2)校验是否为nginx和json日志（"msgSource") == "ngx_log" && line.getString("msgFormat") == "json"） && line.getJSONObject("msgBody")

二、针对crach日志处理，已经平展化
crach 日志包含 STACK_TRACE字段，只有whaley main（appId:boikgpokn78sb95kjhfrendo8dc5mlsr）
和medusa 3.x(appId:boikgpokn78sb95ktmsc1bnkechpgj9l)包含,medsua 2.0包含但是md5校验肯定不成功
处理逻辑
1).判断是否包含STACK_TRACE
2)包含STACK_TRACE，若为whaley 不需要校验，直接添加logId,logSignFlag=0,svr_receive_time 转换成logTime,appId提出，输出
boikgpokn78sb95kjhfrendo8dc5mlsr
{"msgId":"AAABXilcgbsKE7pKAjj2/QAB","msgVersion":"1.0","msgSite":"10.19.186.74","msgSource":"ngx_log","msgFormat":"json","msgSignFlag":0,"msgBody":{"svr_host":"crashlog.aginomoto.com","svr_req_method":"POST","svr_req_url":"/crashlog","svr_content_type":"application/json","svr_remote_addr":"10.9.252.150","svr_forwarded_for":"36.250.86.237","svr_receive_time":1503932481979,"appId":"boikgpokn78sb95kjhfrendo8dc5mlsr","body":{"APP_VERSION_CODE":314,"APP_VERSION_NAME":"1.2.7","PRODUCT_CODE":"55PUF6301\/T3","CRASH_KEY":"java.lang.RuntimeException: Error receiving broadcast Intent { act=com.helios.voice.action.query flg=0x10 pkg=com.helios.vod } in com.helios.l.b.b@2221c47c<br\/>android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:871)<br\/>java.lang.NullPointerException: Attempt to invoke virtual method 'java.lang.String com.helios.l.a.a.a()' on a null object reference<br\/>com.helios.l.b.a.a(Unknown Source)<br\/>","STACK_TRACE":"java.lang.RuntimeException: Error receiving broadcast Intent { act=com.helios.voice.action.query flg=0x10 pkg=com.helios.vod } in com.helios.l.b.b@2221c47c<br\/>\tat android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:871)<br\/>\tat android.os.Handler.handleCallback(Handler.java:739)<br\/>\tat android.os.Handler.dispatchMessage(Handler.java:95)<br\/>\tat android.os.Looper.loop(Looper.java:135)<br\/>\tat android.app.ActivityThread.main(ActivityThread.java:5236)<br\/>\tat java.lang.reflect.Method.invoke(Native Method)<br\/>\tat java.lang.reflect.Method.invoke(Method.java:372)<br\/>\tat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:902)<br\/>\tat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:697)<br\/>Caused by: java.lang.NullPointerException: Attempt to invoke virtual method 'java.lang.String com.helios.l.a.a.a()' on a null object reference<br\/>\tat com.helios.l.b.a.a(Unknown Source)<br\/>\tat com.helios.l.b.b.onReceive(Unknown Source)<br\/>\tat android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:861)<br\/>\t... 8 more<br\/>java.lang.NullPointerException: Attempt to invoke virtual method 'java.lang.String com.helios.l.a.a.a()' on a null object reference<br\/>\tat com.helios.l.b.a.a(Unknown Source)<br\/>\tat com.helios.l.b.b.onReceive(Unknown Source)<br\/>\tat android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:861)<br\/>\tat android.os.Handler.handleCallback(Handler.java:739)<br\/>\tat android.os.Handler.dispatchMessage(Handler.java:95)<br\/>\tat android.os.Looper.loop(Looper.java:135)<br\/>\tat android.app.ActivityThread.main(ActivityThread.java:5236)<br\/>\tat java.lang.reflect.Method.invoke(Native Method)<br\/>\tat java.lang.reflect.Method.invoke(Method.java:372)<br\/>\tat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:902)<br\/>\tat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:697)<br\/>","MAC":"9c:a6:9d:1b:38:c9","PACKAGE_NAME":"com.helios.vod","ANDROID_VERSION":"5.0.2","USER_CRASH_DATE":1503932466638,"THREAD_DETAILS":"main","PLATFORM_CODE":"HELIOS_PHILIPSR-01.24.02.1708210","WUI":"01.24.02"}}}
3)包含STACK_TRACE，若为medusa 3.x 需要校验，校验不成功直接丢弃,
成功添加logId，logSignFlag=1,svr_receive_time， 转换成logTime,appId提出，输出
4)包含STACK_TRACE但appId不是whaley 和medsua 3.x，直接丢弃
5)不包含STACK_TRACE，由get和post处理

三、get post 日志处理逻辑
判断是get还是post
1).get处理
    //{"msgSignFlag":0,"msgId":"AAABXiSPMrMKE2uqS0PalwAB","msgFormat":"json","msgBody":{"svr_content_type":"-","svr_forwarded_for":"123.59.77.3","svr_host":"vrlog.aginomoto.com","svr_remote_addr":"10.10.251.96","svr_receive_time":1503851918003,"appId":"boikgpokn78sb95kbqei6cc98dc5mlsr","svr_req_url":"/uploadlog?log=live-001-MoreTV_TVApp2.0_Android_2.5.9-f360a9dcc80919e308f8465760df9f37--760-home-TVlive-1-abx07nopuwnp-28-guanwang-101010300-MiBOX2-20161111192220","body":{},"svr_req_method":"GET"},"msgSource":"ngx_log","msgVersion":"1.0","msgSite":"10.19.107.170"}
    1.get收集一条日志，处理后生成还是一条。get日志也无需md5校验,logSignFlag=0,svr_receive_time 转换成logTime,appId提出
    2.解析url,从请求的url中参数解析出queryObj 成jsonObject,解析失败直接丢弃
    3.queryObj 放入 logBody中
    4.msgBody 移除body ，同时放入logBody中
    5.生成logId 放入message中
    6.成logBody中提升字段如appId



2.)post处理
    //{"msgSignFlag":1,"msgId":"AAABW+Jaor8KE7pKAgygiQAB","msgFormat":"json","logId":"AAABW+Jaor8KE7pKAgygiQAB","msgBody":{"svr_content_type":"application/json; charset=utf-8","svr_forwarded_for":"111.14.152.46","svr_host":"wlslog.aginomoto.com","svr_remote_addr":"10.10.251.78","svr_receive_time":1494151242431,"appId":"boikgpokn78sb95kjhfrendoj8ilnoi7","svr_req_url":"/log/boikgpokn78sb95kjhfrendoj8ilnoi7","body":{"baseInfo":"{\"sessionId\":\"ee3d5fe9df6591054de6454b4c5e794f\",\"appId\":\"boikgpokn78sb95kjhfrendoj8ilnoi7\",\"productSN\":\"KA1620AB002157D038\",\"androidVersion\":\"Android_SDK_21\",\"userId\":\"6719adce12ec74deb0389b9de11cf429\",\"accountId\":\"90749477\",\"currentVipLevel\":\"basic\",\"productLine\":\"helios\",\"romVersion\":\"01.26.02\",\"productModel\":\"WTV55K1\",\"firmwareVersion\":\"HELIOSR-01.26.02.1714402\",\"sysPlatform\":\"nonyunos\"}","logVersion":"01","logs":"[{\"eventId\":\"helios-launcher-homerefresh\",\"trigger\":\"autoRefresh\",\"pageId\":\"home\",\"logCount\":\"7\",\"pageType\":\"home\",\"happenTime\":\"1167652829247\"}]"},"svr_req_method":"POST"},"msgSource":"ngx_log","msgVersion":"1.0","msgSite":"10.19.186.74"}
    1.post 收集一条日志，处理后生成多条，针对version为01 且msgSignFlag需要校验。
    2.校验（校验失败数据丢弃）
        2.1 version=="01" && msgSignFlag==0 需要md5校验
        2.2 校验字段 baseInfo 、logs 、 ts 、 md5 ,body中必然包含这几个字段，如不存在 。直接丢弃
        2.3 校验表达式 ： ts + baseInfo + logs + key
    3.校验成功 logSignFlag=1
    4.不需要校验logSignFlag=0
    5.校验后，数据平展化（1条记录，平展化后可能生成多条）
        3.1 message中获取msgBody，同时把msgBody移除
        3.2 msgBody中获取body，同时把body移除
        3.3 body中获取baseInfo，同时把baseInfo移除。把 baseInfo 信息放入到body中
        3.5 把body信息展开放入msgBody中

        3.6 把baseInfo日志展开到body中
        3.7 logs（数组）展开成多条log
            3.6.1 把 body 信息放入到log中
            3.6.2 把 log的信息放入到 msgBody
            3.6.3 针对每个log生成一个logId
            3.6.4 创建一个json对象entity,把message剩余的信息放入
            3.6.5 entity 放入logId
            3.6.6 entity.put("logBody",msgBody)


三、时间校验




