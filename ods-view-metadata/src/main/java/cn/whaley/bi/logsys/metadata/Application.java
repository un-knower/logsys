package cn.whaley.bi.logsys.metadata;

import cn.whaley.bi.logsys.metadata.entity.AppLogKeyFieldDescEntity;
import cn.whaley.bi.logsys.metadata.service.ODSViewService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/14.
 *
 * @SpringBootApplication = @Configuration + @EnableAutoConfiguration + @ComponentScan
 * 使用war包需要继承SpringBootServletInitializer
 * 通过EnableAutoConfiguration禁止自动注入,解决多个数据源自动注入冲突
 */
@SpringBootApplication
@ComponentScan(basePackages = {"cn.whaley.bi.logsys.metadata.service", "cn.whaley.bi.logsys.metadata.repository"})
@ImportResource(locations = {"classpath:application-bean.xml"})
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
public class Application {

    public static final Logger LOG = LoggerFactory.getLogger(ODSViewService.class);

    public static Map<String, String> parseArgs(String... args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                Integer idx = arg.indexOf('=');
                String key = arg.substring(2, idx);
                String value = arg.substring(idx + 1);
                params.put(key, value);
            }
        }
        return params;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }


    @Component
    public static class runner implements CommandLineRunner {

        @Autowired
        ODSViewService odsViewService;

        /**
         * --taskId : taskId
         * --disableGenerateDDLAndDML:  true/false, 指示是否禁用GenerateDDLAndDML操作
         * --disableExecuteDDL:  true/false, 指示是否禁用ExecuteDLL操作
         * --disableExecuteDML:  true/false, 指示是否禁用ExecuteDML操作
         *
         * @param strings
         * @throws Exception
         */
        @Override
        public void run(String... strings) throws Exception {
            LOG.info("params:" + StringUtils.join(strings, " "));
            Map<String, String> params = parseArgs(strings);

            String taskId = params.get("taskId");
            Boolean disableGenerateDDLAndDML = Boolean.parseBoolean(params.getOrDefault("disableGenerateDDLAndDML", "false"));
            Boolean disableExecuteDDL = Boolean.parseBoolean(params.getOrDefault("disableExecuteDDL", "false"));
            Boolean disableExecuteDML = Boolean.parseBoolean(params.getOrDefault("disableExecuteDML", "false"));

            Assert.hasText(taskId, "taskId required!");

            LOG.info("processing. taskId:" + taskId);

            if (!disableGenerateDDLAndDML) {
                Integer[] ddlAndDMLRet = odsViewService.generateDDLAndDML(taskId);
                LOG.info("generateDDLAndDML:" + Arrays.stream(ddlAndDMLRet).collect(Collectors.summingInt(item -> item)));
            }
            if (!disableExecuteDDL) {
                Integer ddlRet = odsViewService.executeDDL(taskId);
                LOG.info("executeDDL:" + ddlRet);
            }
            if (!disableExecuteDML) {
                Integer dmlRet = odsViewService.executeDML(taskId);
                LOG.info("executeDML:" + dmlRet);
            }

            LOG.info("done. taskId:" + taskId);

        }
    }

}
