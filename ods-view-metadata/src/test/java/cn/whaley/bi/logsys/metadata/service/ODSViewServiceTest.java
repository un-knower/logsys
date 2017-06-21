package cn.whaley.bi.logsys.metadata.service;

import cn.whaley.bi.logsys.metadata.entity.*;
import cn.whaley.bi.logsys.metadata.repository.LogTabDDLRepo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by fj on 2017/6/19.
 */
public class ODSViewServiceTest {

    public static final Logger LOG = LoggerFactory.getLogger(ODSViewServiceTest.class);


    ODSViewService service;

    @Before
    public void init() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-bean.xml");
        //context.getAutowireCapableBeanFactory().
        service = context.getBean(ODSViewService.class);
    }

    @Test
    public void test1() {
        String taskId = "task1";

        List<AppLogKeyFieldDescEntity> appLogKeyFieldDescEntities = service.getAppLogKeyFieldDescRepo().findAll();
        List<LogFileKeyFieldDescEntity> logFileKeyFieldDescEntities = service.getLogFileKeyFieldDescRepo().findByTaskId(taskId);
        List<LogFileFieldDescEntity> logFileFieldDescEntities = service.getLogFileFieldDescRepo().findByTaskId(taskId);

        //扫描表字段元数据
        List<ODSViewService.TabFieldDescItem> tabFieldDescItems = service.generateTabFieldDesc(appLogKeyFieldDescEntities, logFileKeyFieldDescEntities, logFileFieldDescEntities);

        for (ODSViewService.TabFieldDescItem descItem : tabFieldDescItems) {
            List<LogTabDDLEntity> ddlEntities = service.generateDDL(descItem);
            for (LogTabDDLEntity ddlEntity : ddlEntities) {
                LOG.info(ddlEntity.getDdlText());
            }
            //保存DDL
            int ret = service.getLogTabDDLRepo().insert(ddlEntities);
            LOG.info("insert ddl:" + ret);
        }

        for (ODSViewService.TabFieldDescItem descItem : tabFieldDescItems) {
            LogTabDMLEntity dmlEntity = service.generateDML(descItem.desc);
            LOG.info(dmlEntity.getDmlText());
            //保存DML
            int ret = service.getLogTabDMLRepo().insert(Arrays.asList(dmlEntity));
            LOG.info("insert dml:" + ret);
        }


        Assert.assertTrue(tabFieldDescItems.size() > 0);
    }

    @Test
    public void test2() {
        String taskId = "task1";
        Integer[] ret = service.generateDDLAndDML(taskId);
        LOG.info("fieldRet:{}, ddlRet:{}, dmlRet:{}", new Object[]{ret[0], ret[1], ret[2]});
        Assert.assertTrue(ret[0] > 0 && ret[1] > 0);
    }

    @Test
    public void test3() {
        String taskId = "task1";
        service.executeDDL(taskId);
    }

    @Test
    public void testGetTabFieldInfo() {
        String taskId = "task1";
        List<HiveFieldInfo> infos = service.getHiveRepo().getTabFieldInfo("test", "test_log_test_product_test_app_test_type_test_event");
        Assert.assertTrue(infos.size() > 0);
    }

    @Test
    public void testX() {
        String taskId = "task1";
        LogTabDDLRepo repo = service.getLogTabDDLRepo();
        Integer ret = repo.deleteByTaskId(taskId);
        LOG.info("ret=" + ret);
    }

}
