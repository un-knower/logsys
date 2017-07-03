package cn.whaley.bi.logsys.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;


/**
 * Created by fj on 2017/6/14.
 *
 * @SpringBootApplication = @Configuration + @EnableAutoConfiguration + @ComponentScan
 * 使用war包需要继承SpringBootServletInitializer
 * 通过EnableAutoConfiguration禁止自动注入,解决多个数据源自动注入冲突
 */
@SpringBootApplication
@ComponentScan(basePackages = {"cn.whaley.bi.logsys.metadata.service"
        , "cn.whaley.bi.logsys.metadata.repository"
        , "cn.whaley.bi.logsys.metadata.controller"
})
@ImportResource(locations = {"classpath:application-bean.xml"})
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
public class Application {

    public static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
