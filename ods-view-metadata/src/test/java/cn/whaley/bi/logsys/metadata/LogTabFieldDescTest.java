package cn.whaley.bi.logsys.metadata;

import cn.whaley.bi.logsys.metadata.repository.LogTabFieldDescRepo;
import cn.whaley.bi.logsys.metadata.entity.LogTabFieldDescEntity;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


import java.util.List;

/**
 * Created by fj on 2017/6/14.
 */
public class LogTabFieldDescTest {

    public static final Logger LOG = LoggerFactory.getLogger(LogTabFieldDescTest.class);


    private LogTabFieldDescRepo repo;

    @Before
    public void init(){
        ApplicationContext context=new  ClassPathXmlApplicationContext("classpath:application.xml");
        repo=context.getBean(LogTabFieldDescRepo.class);
    }

    @Test
    public void Test1() {
        List<LogTabFieldDescEntity> entities = repo.findByTaskId("");
        LOG.info("entities:" + entities.size());
    }


}
