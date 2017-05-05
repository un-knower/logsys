import cn.whaley.bi.logsys.common.StringDecoder;
import org.junit.Test;

/**
 * Created by fj on 17/4/28.
 */
public class StringDecoderTest {
    @Test
    public void test1(){
        String str="{\"msgId\":\"AAABW7K28noKEz1LE8p3dQAB\",\"msgVersion\":\"1.0\",\"msgSite\":\"10.19.61.75\",\"msgSource\":\"ngx_log\",\"msgFormat\":\"json\",\"msgSignFlag\":1,\"msgBody\":{\"svr_host\":\"wlslog.aginomoto.com\",\"svr_req_method\":\"POST\",\"svr_req_url\":\"/log/boikgpokn78sb95kjhfrendoj8ilnoi7\",\"svr_content_type\":\"application/json; charset=utf-8\",\"svr_remote_addr\":\"10.10.251.81\",\"svr_forwarded_for”:”61.129.122.66\",\"svr_receive_time\":1493351985786,\"appId\":\"boikgpokn78sb95kjhfrendoj8ilnoi7\",\"body\":{\\x22logs\\x22:\\x22[{\\x5C\\x22eventId\\x5C\\x22:\\x5C\\x22navigationBarMove\\x5C\\x22,\\x5C\\x22pageId\\x5C\\x22:\\x5C\\x22home\\x5C\\x22,\\x5C\\x22logCount\\x5C\\x22:\\x5C\\x222538\\x5C\\x22,\\x5C\\x22naviName\\x5C\\x22:\\x5C\\x22\\xE5\\x88\\x86\\xE7\\xB1\\xBB\\x5C\\x22,\\x5C\\x22pageType\\x5C\\x22:\\x5C\\x22home\\x5C\\x22,\\x5C\\x22happenTime\\x5C\\x22:\\x5C\\x221493351985748\\x5C\\x22}]\\x22,\\x22baseInfo\\x22:\\x22{\\x5C\\x22sessionId\\x5C\\x22:\\x5C\\x220b6244aa281682b34a8a937958771ca3\\x5C\\x22,\\x5C\\x22appId\\x5C\\x22:\\x5C\\x22boikgpokn78sb95kjhfrendoj8ilnoi7\\x5C\\x22,\\x5C\\x22productSN\\x5C\\x22:\\x5C\\x22XX1501H01P43DFT001\\x5C\\x22,\\x5C\\x22androidVersion\\x5C\\x22:\\x5C\\x22Android_SDK_22\\x5C\\x22,\\x5C\\x22userId\\x5C\\x22:\\x5C\\x22974a39d74825ea8465fdd0c9e1395d16\\x5C\\x22,\\x5C\\x22accountId\\x5C\\x22:\\x5C\\x22\\x5C\\x22,\\x5C\\x22currentVipLevel\\x5C\\x22:\\x5C\\x22basic\\x5C\\x22,\\x5C\\x22productLine\\x5C\\x22:\\x5C\\x22helios\\x5C\\x22,\\x5C\\x22romVersion\\x5C\\x22:\\x5C\\x2201.26.02\\x5C\\x22,\\x5C\\x22productModel\\x5C\\x22:\\x5C\\x22W49F\\x5C\\x22,\\x5C\\x22firmwareVersion\\x5C\\x22:\\x5C\\x22APOLLOR-01.26.02.1714402\\x5C\\x22,\\x5C\\x22sysPlatform\\x5C\\x22:\\x5C\\x22nonyunos\\x5C\\x22}\\x22,\\x22logVersion\\x22:\\x2201\\x22}}}";
        StringDecoder decoder=new StringDecoder();
        String decodedStr= decoder.decodeToString(str);
        System.out.println(str);
        System.out.println(decodedStr);
    }
}
