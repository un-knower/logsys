package cn.whaley.bi.logsys.metadata;

import ch.qos.logback.core.net.SyslogOutputStream;
import cn.whaley.bi.logsys.metadata.entity.LogBaseInfoEntity;
import cn.whaley.bi.logsys.metadata.repository.LogBaseInfoRepo;
import cn.whaley.bi.logsys.metadata.repository.LogTabFieldDescRepo;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;

/**
 * Created by guohao on 2017/10/18.
 */
public class LogBaseInfoTest {
    public static final Logger LOG = LoggerFactory.getLogger(LogBaseInfoTest.class);

    LogBaseInfoRepo repo;

    @Before
    public void init(){
        ApplicationContext context=new ClassPathXmlApplicationContext("classpath:application-bean.xml");
        repo=context.getBean(LogBaseInfoRepo.class);
    }

    @Test
    public void test1(){
        List<LogBaseInfoEntity> baseInfoEntities = repo.findAll();
        System.out.println(baseInfoEntities.size());
    }

}
