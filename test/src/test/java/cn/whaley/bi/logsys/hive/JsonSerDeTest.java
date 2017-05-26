package cn.whaley.bi.logsys.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by fj on 17/4/24.
 */
public class JsonSerDeTest {


    @Test
    public void test4() throws SerDeException {
        Configuration conf = new Configuration();
        Properties props = new Properties();
        props.put(serdeConstants.LIST_COLUMNS, "@timestamp,agent");
        props.put(serdeConstants.LIST_COLUMN_TYPES, "string,string");
        props.put(JsonSerDe.SERDE_SERIALIZATION_ROW_DECODE_REQUIRE, "true");
        JsonSerDe rjsd = new JsonSerDe();
        SerDeUtils.initializeSerDe(rjsd, conf, props, null);

        String text2="{\"@timestamp\":\"2017-04-10T04:29:04+08:00\",\"host\":\"10.10.219.224\",\"clientip\":\"115.153.176.57\",\"size\":985,\"responsetime\":0.001,\"upstreamtime\":\"-\",\"upstreamhost\":\"-\",\"http_host\":\"rec.tvmore.com.cn\",\"url\":\"GET /v3/Service/HomePage?userId=2043b8214c89c2a714c6ef31bee93be8&accountId=&promotionChannel=&modelGroupCode=&acode=2806 HTTP/1.1\",\"xff\":\"-\",\"referer\":\"-\",\"agent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) \\x09\\x09\\x09Chrome/55.0.2883.95 Safari/537.36\",\"status\":\"200\"}";
        HCatRecord record=(HCatRecord)rjsd.deserialize(new Text(text2));
        System.out.println(record.toString());
    }

    @Test
    public void test5() throws Exception{
        Configuration conf = new Configuration();
        Properties props = new Properties();
        props.put(serdeConstants.LIST_COLUMNS, "url");
        props.put(serdeConstants.LIST_COLUMN_TYPES, "string");
        props.put(JsonSerDe.SERDE_SERIALIZATION_ROW_DECODE_REQUIRE, "true");
        props.put(JsonSerDe.SERDE_SERIALIZATION_ROW_DECODE_ALLOW_UNQUOTED_BACKSLASH, "true");
        JsonSerDe rjsd = new JsonSerDe();
        SerDeUtils.initializeSerDe(rjsd, conf, props, null);

        String text1="{\"@timestamp\":\"2017-04-10T10:19:55+08:00\",\"host\":\"10.10.219.224\",\"clientip\":\"179.197.216.90\",\"size\":166,\"responsetime\":0.347,\"upstreamtime\":\"-\",\"upstreamhost\":\"-\",\"http_host\":\"rec.moretv.com.cn\",\"url\":\"\\x91\\x00\\x00\\x00\\x22k\\x12\\x8B\\xC5<R\\x12$\\x8F#R\\x1D\\xFDW/\\x95\\xFBx\\xA7\\xBA\\x18\\x9C\\xD1\\x16t\\xD8\\xF6\\xFB\\x96\\xF8\\xEE\\xF9$\\xD4e\\x8B\\x86#\\xA3\\xC9Ct\\xB7\\x84\\xC9\\xB4jXh\\x11:\\xF4\\xED\\xAD\\xF4\\x97\\xFD\\xA9\\xD3\\xB1\\xB9\\xD6m\\xC3\\x8C\\xB0E4!>Z\\x9B\\xDC\\x1C\\xB4QX\\xB6\\xCD\\x07\\xEC\\x7F\\x8C\\xD23\\x05\\xC6\\x91\\x13\\x82\\xEDS4\\xCC\\xF2\\xB9\\xD49\\xF0Pw\\xBF\\x19uc\\xE6u[h$\\x86W\\xD7\\xCB\\xB8JAC.\\xCBz,\\xE8\\x8F\\xA1R\\xFDb\\x98\\xC9\\xBF\\xD8\\xC1\\xFB\\x88\\x9D7\\x07\\x17o\\x09\\xDB\\xEB\\x84\",\"xff\":\"-\",\"referer\":\"-\",\"agent\":\"-\",\"status\":\"400\"}";
        System.out.println(text1);
        //text1=text1.replace("\\","\\\\");
        //System.out.println(text1);
        //System.out.println(NgxStyleStrDecoder.decodeToString(text1));

        HCatRecord record=(HCatRecord)rjsd.deserialize(new Text(text1));
        System.out.println(record.toString());
    }


}
