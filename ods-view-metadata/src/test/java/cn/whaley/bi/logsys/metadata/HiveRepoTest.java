package cn.whaley.bi.logsys.metadata;


import cn.whaley.bi.logsys.metadata.repository.HiveRepo;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * Created by fj on 2017/6/16.
 */
public class HiveRepoTest {
    public static final Logger LOG = LoggerFactory.getLogger(LogTabFieldDescTest.class);


    private HiveRepo repo;

    @Before
    public void init() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-bean.xml");
        repo = context.getBean(HiveRepo.class);
    }

    @Test
    public void Test1() {
        String dbName = "ods";
        String tabName = "t_metadata_log";
        Boolean exists = repo.tabExists(dbName, tabName);
        LOG.info(String.format("%s.%s exists:%s", dbName, tabName, exists));
    }


}
