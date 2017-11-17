package cn.whaley.bi.logsys.metadata;

import cn.whaley.bi.logsys.metadata.util.SendMail;
import org.junit.Test;

/**
 * Created by guohao on 2017/11/17.
 */
public class Mail {
    @Test
    public void test(){
        String[] users = {"app-bigdata@whaley.cn"};
        SendMail.post("test1111","test",users);
    }
}
