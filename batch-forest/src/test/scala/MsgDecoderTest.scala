import cn.whaley.bi.logsys.batchforest.traits.LogTrait
import cn.whaley.bi.logsys.batchforest.util.MsgDecoder
import com.alibaba.fastjson.JSON
import org.junit.Test

/**
  * Created by guohao on 2017/8/30.
  */
class MsgDecoderTest extends LogTrait{

  @Test
  def testCrach(): Unit ={
    //whaley crach
    // val line = "{\"msgId\":\"AAABXilcgbsKE7pKAjj2/QAB\",\"msgVersion\":\"1.0\",\"msgSite\":\"10.19.186.74\",\"msgSource\":\"ngx_log\",\"msgFormat\":\"json\",\"msgSignFlag\":0,\"msgBody\":{\"svr_host\":\"crashlog.aginomoto.com\",\"svr_req_method\":\"POST\",\"svr_req_url\":\"/crashlog\",\"svr_content_type\":\"application/json\",\"svr_remote_addr\":\"10.9.252.150\",\"svr_forwarded_for\":\"36.250.86.237\",\"svr_receive_time\":1503932481979,\"appId\":\"boikgpokn78sb95kjhfrendo8dc5mlsr\",\"body\":{\\x22APP_VERSION_CODE\\x22:314,\\x22APP_VERSION_NAME\\x22:\\x221.2.7\\x22,\\x22PRODUCT_CODE\\x22:\\x2255PUF6301\\x5C/T3\\x22,\\x22CRASH_KEY\\x22:\\x22java.lang.RuntimeException: Error receiving broadcast Intent { act=com.helios.voice.action.query flg=0x10 pkg=com.helios.vod } in com.helios.l.b.b@2221c47c<br\\x5C/>android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:871)<br\\x5C/>java.lang.NullPointerException: Attempt to invoke virtual method 'java.lang.String com.helios.l.a.a.a()' on a null object reference<br\\x5C/>com.helios.l.b.a.a(Unknown Source)<br\\x5C/>\\x22,\\x22STACK_TRACE\\x22:\\x22java.lang.RuntimeException: Error receiving broadcast Intent { act=com.helios.voice.action.query flg=0x10 pkg=com.helios.vod } in com.helios.l.b.b@2221c47c<br\\x5C/>\\x5Ctat android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:871)<br\\x5C/>\\x5Ctat android.os.Handler.handleCallback(Handler.java:739)<br\\x5C/>\\x5Ctat android.os.Handler.dispatchMessage(Handler.java:95)<br\\x5C/>\\x5Ctat android.os.Looper.loop(Looper.java:135)<br\\x5C/>\\x5Ctat android.app.ActivityThread.main(ActivityThread.java:5236)<br\\x5C/>\\x5Ctat java.lang.reflect.Method.invoke(Native Method)<br\\x5C/>\\x5Ctat java.lang.reflect.Method.invoke(Method.java:372)<br\\x5C/>\\x5Ctat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:902)<br\\x5C/>\\x5Ctat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:697)<br\\x5C/>Caused by: java.lang.NullPointerException: Attempt to invoke virtual method 'java.lang.String com.helios.l.a.a.a()' on a null object reference<br\\x5C/>\\x5Ctat com.helios.l.b.a.a(Unknown Source)<br\\x5C/>\\x5Ctat com.helios.l.b.b.onReceive(Unknown Source)<br\\x5C/>\\x5Ctat android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:861)<br\\x5C/>\\x5Ct... 8 more<br\\x5C/>java.lang.NullPointerException: Attempt to invoke virtual method 'java.lang.String com.helios.l.a.a.a()' on a null object reference<br\\x5C/>\\x5Ctat com.helios.l.b.a.a(Unknown Source)<br\\x5C/>\\x5Ctat com.helios.l.b.b.onReceive(Unknown Source)<br\\x5C/>\\x5Ctat android.app.LoadedApk$ReceiverDispatcher$Args.run(LoadedApk.java:861)<br\\x5C/>\\x5Ctat android.os.Handler.handleCallback(Handler.java:739)<br\\x5C/>\\x5Ctat android.os.Handler.dispatchMessage(Handler.java:95)<br\\x5C/>\\x5Ctat android.os.Looper.loop(Looper.java:135)<br\\x5C/>\\x5Ctat android.app.ActivityThread.main(ActivityThread.java:5236)<br\\x5C/>\\x5Ctat java.lang.reflect.Method.invoke(Native Method)<br\\x5C/>\\x5Ctat java.lang.reflect.Method.invoke(Method.java:372)<br\\x5C/>\\x5Ctat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:902)<br\\x5C/>\\x5Ctat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:697)<br\\x5C/>\\x22,\\x22MAC\\x22:\\x229c:a6:9d:1b:38:c9\\x22,\\x22PACKAGE_NAME\\x22:\\x22com.helios.vod\\x22,\\x22ANDROID_VERSION\\x22:\\x225.0.2\\x22,\\x22USER_CRASH_DATE\\x22:1503932466638,\\x22THREAD_DETAILS\\x22:\\x22main\\x22,\\x22PLATFORM_CODE\\x22:\\x22HELIOS_PHILIPSR-01.24.02.1708210\\x22,\\x22WUI\\x22:\\x2201.24.02\\x22}}}"
    //medusa crach
     val line = "{\"msgId\":\"AAABXijuSpsKE2uqIMNkEwAB\",\"msgVersion\":\"1.0\",\"msgSite\":\"10.19.107.170\",\"msgSource\":\"ngx_log\",\"msgFormat\":\"json\",\"msgSignFlag\":0,\"msgBody\":{\"svr_host\":\"medusalog.aiseewhaley.aisee.tv\",\"svr_req_method\":\"POST\",\"svr_req_url\":\"/crashlog\",\"svr_content_type\":\"application/json\",\"svr_remote_addr\":\"10.19.251.56\",\"svr_forwarded_for\":\"115.46.208.136\",\"svr_receive_time\":1503925258907,\"appId\":\"boikgpokn78sb95ktmsc1bnkechpgj9l\",\"body\":{\\x22DATE_CODE\\x22:20170731,\\x22APP_VERSION_NAME\\x22:\\x223.1.5\\x22,\\x22USER_CRASH_DATE\\x22:1503925248941,\\x22THREAD_DETAILS\\x22:\\x22main\\x22,\\x22MD5\\x22:\\x222f7a178dda181d5a5299f35a967fd6be\\x22,\\x22STACK_TRACE\\x22:\\x22java.lang.UnsatisfiedLinkError: Native method not found: com.tencent.mars.BaseEvent.onForeground:(Z)V<br\\x5C/>\\x5Ctat com.tencent.mars.BaseEvent.onForeground(Native Method)<br\\x5C/>\\x5Ctat com.f.a.a.e.c.b(SourceFile:102)<br\\x5C/>\\x5Ctat com.f.a.a.f.run(SourceFile:129)<br\\x5C/>\\x5Ctat android.os.Handler.handleCallback(Handler.java:733)<br\\x5C/>\\x5Ctat android.os.Handler.dispatchMessage(Handler.java:95)<br\\x5C/>\\x5Ctat android.os.Looper.loop(Looper.java:136)<br\\x5C/>\\x5Ctat android.app.ActivityThread.main(ActivityThread.java:5018)<br\\x5C/>\\x5Ctat java.lang.reflect.Method.invokeNative(Native Method)<br\\x5C/>\\x5Ctat java.lang.reflect.Method.invoke(Method.java:515)<br\\x5C/>\\x5Ctat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:828)<br\\x5C/>\\x5Ctat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:644)<br\\x5C/>\\x5Ctat dalvik.system.NativeStart.main(Native Method)<br\\x5C/>\\x22,\\x22PRODUCT_CODE\\x22:\\x22MiBOX1S\\x22,\\x22MAC\\x22:\\x2210:48:b1:11:f1:9b\\x22,\\x22ANDROID_VERSION\\x22:\\x224.4.2\\x22,\\x22CUSTOM_JSON_DATA\\x22:\\x22[{\\x5C\\x22className\\x5C\\x22:\\x5C\\x22MoreTvApplication\\x5C\\x22,\\x5C\\x22time\\x5C\\x22:\\x5C\\x2208-28 21:00:41.616\\x5C\\x22,\\x5C\\x22threadName\\x5C\\x22:\\x5C\\x22main\\x5C\\x22,\\x5C\\x22msg\\x5C\\x22:\\x5C\\x22onCreate\\x5C\\x22,\\x5C\\x22methodName\\x5C\\x22:\\x5C\\x22onCreate\\x5C\\x22,\\x5C\\x22tag\\x5C\\x22:\\x5C\\x22TAG_ACTIVITY\\x5C\\x22},{\\x5C\\x22className\\x5C\\x22:\\x5C\\x22StartActivity\\x5C\\x22,\\x5C\\x22time\\x5C\\x22:\\x5C\\x2208-28 21:00:41.929\\x5C\\x22,\\x5C\\x22threadName\\x5C\\x22:\\x5C\\x22main\\x5C\\x22,\\x5C\\x22msg\\x5C\\x22:\\x5C\\x22init\\x5C\\x22,\\x5C\\x22methodName\\x5C\\x22:\\x5C\\x22init\\x5C\\x22,\\x5C\\x22tag\\x5C\\x22:\\x5C\\x22TAG_ACTIVITY\\x5C\\x22},{\\x5C\\x22className\\x5C\\x22:\\x5C\\x22StartActivity\\x5C\\x22,\\x5C\\x22time\\x5C\\x22:\\x5C\\x2208-28 21:00:43.490\\x5C\\x22,\\x5C\\x22threadName\\x5C\\x22:\\x5C\\x22main\\x5C\\x22,\\x5C\\x22msg\\x5C\\x22:\\x5C\\x22onCreate\\x5C\\x22,\\x5C\\x22methodName\\x5C\\x22:\\x5C\\x22onCreate\\x5C\\x22,\\x5C\\x22tag\\x5C\\x22:\\x5C\\x22TAG_ACTIVITY\\x5C\\x22},{\\x5C\\x22className\\x5C\\x22:\\x5C\\x22SingleActivity\\x5C\\x22,\\x5C\\x22time\\x5C\\x22:\\x5C\\x2208-28 21:00:45.427\\x5C\\x22,\\x5C\\x22threadName\\x5C\\x22:\\x5C\\x22main\\x5C\\x22,\\x5C\\x22msg\\x5C\\x22:\\x5C\\x22onCreate\\x5C\\x22,\\x5C\\x22methodName\\x5C\\x22:\\x5C\\x22onCreate\\x5C\\x22,\\x5C\\x22tag\\x5C\\x22:\\x5C\\x22TAG_ACTIVITY\\x5C\\x22},{\\x5C\\x22className\\x5C\\x22:\\x5C\\x22StartPageActivity\\x5C\\x22,\\x5C\\x22time\\x5C\\x22:\\x5C\\x2208-28 21:00:46.536\\x5C\\x22,\\x5C\\x22threadName\\x5C\\x22:\\x5C\\x22main\\x5C\\x22,\\x5C\\x22msg\\x5C\\x22:\\x5C\\x22construct - onCreate - onStart - onResume\\x5C\\x22,\\x5C\\x22methodName\\x5C\\x22:\\x5C\\x22run\\x5C\\x22,\\x5C\\x22tag\\x5C\\x22:\\x5C\\x22TAG_PAGE\\x5C\\x22}]\\x22,\\x22CRASH_KEY\\x22:\\x22java.lang.UnsatisfiedLinkError: Native method not found: com.tencent.mars.BaseEvent.onForeground:(Z)V<br\\x5C/>com.tencent.mars.BaseEvent.onForeground(Native Method)<br\\x5C/>\\x22,\\x22APP_VERSION_CODE\\x22:315}}}"
    val msg =  MsgDecoder.decode(line).get
    val msgJson = JSON.parseObject(msg)
    println(msgJson.containsKey("msgId"))
    println(msgJson.toString.contains("STACK_TRACE"))
  }
  @Test
  def test(): Unit ={
    val line = "{\"msgId\":\"AAABXijtbDMKE2uqIMX7MAAB\",\"msgVersion\":\"1.0\",\"msgSite\":\"10.19.107.170\",\"msgSource\":\"ngx_log\",\"msgFormat\":\"json\",\"msgSignFlag\":0,\"msgBody\":{\"svr_host\":\"log3.tvmore.com.cn\",\"svr_req_method\":\"POST\",\"svr_req_url\":\"/medusalog/\",\"svr_content_type\":\"application/json\",\"svr_remote_addr\":\"10.10.251.83\",\"svr_forwarded_for\":\"113.142.199.132\",\"svr_receive_time\":1503925201971,\"appId\":\"boikgpokn78sb95ktmsc1bnkechpgj9l\",\"body\":{\\x22md5\\x22:\\x223be2f1cd77c342b6d50b2d1161996e78\\x22,\\x22baseInfo\\x22:\\x22{\\x5C\\x22apkVersion\\x5C\\x22:\\x5C\\x223.1.3\\x5C\\x22,\\x5C\\x22groupId\\x5C\\x22:\\x5C\\x22601\\x5C\\x22,\\x5C\\x22accountId\\x5C\\x22:\\x5C\\x22\\x5C\\x22,\\x5C\\x22appEnterWay\\x5C\\x22:\\x5C\\x22native\\x5C\\x22,\\x5C\\x22promotionChannel\\x5C\\x22:\\x5C\\x22guanwang\\x5C\\x22,\\x5C\\x22userId\\x5C\\x22:\\x5C\\x2210abf8397121795e75edea3079c520a8\\x5C\\x22,\\x5C\\x22apkSeries\\x5C\\x22:\\x5C\\x22MoreTV_TVApp3.0_Medusa\\x5C\\x22,\\x5C\\x22productModel\\x5C\\x22:\\x5C\\x22GenericAndroidonmt5882\\x5C\\x22,\\x5C\\x22versionCode\\x5C\\x22:\\x5C\\x22313\\x5C\\x22,\\x5C\\x22buildDate\\x5C\\x22:\\x5C\\x2220170206\\x5C\\x22,\\x5C\\x22uploadTime\\x5C\\x22:\\x5C\\x2220170828210001\\x5C\\x22,\\x5C\\x22weatherCode\\x5C\\x22:\\x5C\\x22101110200\\x5C\\x22}\\x22,\\x22ts\\x22:1503925201801,\\x22logs\\x22:\\x22[{\\x5C\\x22logVersion\\x5C\\x22:\\x5C\\x2202\\x5C\\x22,\\x5C\\x22happenTime\\x5C\\x22:1503924938401,\\x5C\\x22params\\x5C\\x22:{\\x5C\\x22pathMain\\x5C\\x22:\\x5C\\x22home*my_tv*history-history\\x5C\\x22,\\x5C\\x22episodeSid\\x5C\\x22:\\x5C\\x225ii63fefcek7\\x5C\\x22,\\x5C\\x22videoSid\\x5C\\x22:\\x5C\\x225iwyrtnog6qt\\x5C\\x22,\\x5C\\x22contentType\\x5C\\x22:\\x5C\\x22zongyi\\x5C\\x22,\\x5C\\x22videoName\\x5C\\x22:\\x5C\\x22[完整版]第7期：宋小宝袁姗姗扮特工夫妻\\x5C\\x22,\\x5C\\x22pathSpecial\\x5C\\x22:\\x5C\\x22\\x5C\\x22,\\x5C\\x22pathSub\\x5C\\x22:\\x5C\\x22\\x5C\\x22,\\x5C\\x22key\\x5C\\x22:\\x5C\\x22longRight\\x5C\\x22},\\x5C\\x22eventId\\x5C\\x22:\\x5C\\x22medusa-keyevent-key\\x5C\\x22,\\x5C\\x22logType\\x5C\\x22:\\x5C\\x22event\\x5C\\x22}]\\x22,\\x22version\\x22:\\x2201\\x22}}}"
    val msg =  MsgDecoder.decode(line).get
    println(msg)
  }
}
