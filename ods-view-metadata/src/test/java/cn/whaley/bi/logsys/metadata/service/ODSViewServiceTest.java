package cn.whaley.bi.logsys.metadata.service;

import cn.whaley.bi.logsys.metadata.entity.*;
import cn.whaley.bi.logsys.metadata.repository.LogTabDDLRepo;
import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.ArrayList;
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
    public void testTabFieldDesc(){
        String taskId="AAABXTAEkRsK4AXxhMAAAAA";
        String appId="boikgpokn78sb95ktmsc1bnkechpgj9l";
        String logPath="data_warehouse/ods_view.db/log_medusa_main3x_enter_medusa_accountcenter_home_login_process/key_day=20170711/key_hour=08";
        List<AppLogKeyFieldDescEntity> appLogKeyFieldDescEntities = service.getAppLogKeyFieldDescRepo().findAll();

        List<LogFileKeyFieldValueEntity> logFileKeyFieldDescEntities = service.getLogFileKeyFieldValueRepo().findByTaskIdAndLogPath(taskId, logPath);
        List<LogFileFieldDescEntity> logFileFieldDescEntities = service.getLogFileFieldDescRepo().findByTaskIdAndLogPath(taskId,logPath);

        List<ODSViewService.TabFieldDescItem> tabFieldDescItems = service.generateTabFieldDesc(appLogKeyFieldDescEntities, logFileKeyFieldDescEntities, logFileFieldDescEntities);

        LOG.info("appId:{},ret={}",appId,tabFieldDescItems.size());
    }


    @Test
    public void testGenerateDDLAndDML() {
        String taskId = "AAABX2su86sKCgEPqi8AAAAA";
        Integer[] ret = service.generateDDLAndDML(taskId);
        LOG.info("fieldRet:{}, ddlRet:{}, dmlRet:{}", new Object[]{ret[0], ret[1], ret[2]});
        Assert.assertTrue(ret[0] >= 0 && ret[1] >= 0);
    }

    @Test
    public void testQueryDML() {
        String taskId = "task1";
        List<LogTabDMLEntity> dmlEntities = service.getLogTabDMLRepo().queryForTaskId(taskId, false);
        LOG.info("dmlEntities:" + dmlEntities.size());
    }


    @Test
    public void testExecuteDDL() {
        String taskId = "task1";
        service.executeDDL(taskId);
    }

    @Test
    public void testExecuteDML() {
        String taskId = "task1";
        service.executeDML(taskId,"false");
    }


    @Test
    public void testGetTabFieldInfo() {
        String taskId = "task1";
        List<HiveFieldInfo> infos = service.getHiveRepo().getTabFieldInfo("test", "test_log_test_product_test_app_test_type_test_event");
        Assert.assertTrue(infos.size() > 0);
    }

    @Test
    public void testDeleteByTaskId() {
        String taskId = "task1";
        LogTabDDLRepo repo = service.getLogTabDDLRepo();
        Integer ret = repo.deleteByTaskId(taskId);
        LOG.info("ret=" + ret);
    }


    @Test
    public void printStr(){
        List<LogFileFieldDescEntity> entities=new ArrayList<>();
        LogFileFieldDescEntity entity=new LogFileFieldDescEntity();
        entity.setFieldName("productCode");
        entity.setFieldSql("`productCode` string");
        entity.setFieldType("string");
        entity.setLogPath("/test/file1.txt");
        entity.setRawInfo("STRING");
        entity.setRawType("BYTE");
        entity.setTaskId("task1");
        entities.add(entity);
        LOG.info(JSON.toJSONString(entities));
    }

}
