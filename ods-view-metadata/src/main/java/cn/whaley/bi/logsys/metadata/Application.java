package cn.whaley.bi.logsys.metadata;

import cn.whaley.bi.logsys.metadata.service.ODSViewService;
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

/**
 * Created by fj on 2017/6/14.
 *
 * @SpringBootApplication = @Configuration + @EnableAutoConfiguration + @ComponentScan
 * 使用war包需要继承SpringBootServletInitializer
 * 通过EnableAutoConfiguration禁止自动注入多个数据源
 */
@SpringBootApplication
@ComponentScan(basePackages = {"cn.whaley.bi.logsys.metadata.service","cn.whaley.bi.logsys.metadata.repository"})
@ImportResource(locations={"classpath:application-bean.xml"})
@EnableAutoConfiguration(exclude = {
DataSourceAutoConfiguration.class
//,HibernateJpaAutoConfiguration.class //（如果使用Hibernate时，需要加）
//,DataSourceTransactionManagerAutoConfiguration.class
})
public class Application {

    public static final Logger LOG = LoggerFactory.getLogger(ODSViewService.class);

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(Application.class, args);
        //SpringApplication application = new SpringApplication(Application.class);
        //application.setWebEnvironment(false);
        //ApplicationContext context = application.run(args);
        LOG.info("OK");
    }


    @Component
    public static class runner implements CommandLineRunner {

        @Autowired
        ODSViewService odsViewService;

        @Override
        public void run(String... strings) throws Exception {
            System.out.println("running...");
        }
    }

    @Controller
    public static class ManagerController {
        //@RequestMapping
        public String submitTask(String taskId) {
            return taskId;
        }
    }

}
