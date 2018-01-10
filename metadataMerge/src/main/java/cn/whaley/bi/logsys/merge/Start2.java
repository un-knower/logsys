package cn.whaley.bi.logsys.merge;

import cn.whaley.bi.logsys.merge.service.HiveService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.SQLException;

/**
 * Created by guohao on 2017/12/14.
 */
public class Start2 {
    public static void main(String[] args) throws SQLException {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-bean.xml");
        HiveService hiveService = context.getBean(HiveService.class);
//        hiveService.getMetaDate();
    }
}
