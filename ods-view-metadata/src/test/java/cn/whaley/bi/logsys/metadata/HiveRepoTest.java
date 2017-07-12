package cn.whaley.bi.logsys.metadata;


import cn.whaley.bi.logsys.metadata.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.metadata.repository.HiveRepo;
import cn.whaley.bi.logsys.metadata.service.HiveUtil;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;
import java.util.function.Consumer;


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

    @Test
    public void Test2() {
        String dbName = "ods_view";
        String tabName = "log_medusa_main3x_switchonoff";
        List<HiveFieldInfo> fieldInfos = repo.getTabFieldInfo(dbName, tabName);
        for (HiveFieldInfo fieldInfo : fieldInfos) {
            LOG.info("{} {} {}", fieldInfo.getPartitionField(), fieldInfo.getColName(), fieldInfo.getDataType());
        }
    }

    @Test
    public void Test3() {
        String oldFieldType = "struct<logSignFlag:bigint,msgFormat:string,msgId:string,msgSignFlag:bigint,msgSite:string,msgSource:string,msgVersion:string>";
        String newFieldType = "struct<logSignFlag:bigint,msgFormat:string,msgId:string,msgSignFlag:bigint,msgSite:string,msgSource:string,msgVersion:string>";
        Boolean convertible = HiveUtil.implicitConvertible(oldFieldType, newFieldType);
        LOG.info("{}:{}->{}", convertible, oldFieldType, newFieldType);
    }


    @Test
    public void Test4() {
        String dbName = "ods_view";
        String tabName = "log_medusa_main3x_start_end";
        List<HiveFieldInfo> fieldInfos = repo.getTabFieldInfo(dbName, tabName);
        fieldInfos.stream().map(item -> item.getColName() + " : " + item.getDataType())
                .forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        LOG.info(s);
                    }
                });
    }


}
