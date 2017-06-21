package cn.whaley.bi.logsys.metadata.config;

import cn.whaley.bi.logsys.metadata.service.ODSViewService;
import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Created by fj on 2017/6/14.
 */

//@Configuration
//@ImportResource(locations={"classpath:application-bean.xml"})
public class AppConf {

    //@Autowired
    //public ODSViewService odsViewService;

    /*

    @Autowired
    private Environment env;

    @Bean
    public DataSource dataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(env.getProperty("phoenix-jdbc.driverClassName"));
        dataSource.setUrl(env.getProperty("phoenix-jdbc.url"));
        dataSource.setUsername(env.getProperty("phoenix-jdbc.username"));
        dataSource.setPassword(env.getProperty("phoenix-jdbc.password"));
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(20);
        dataSource.setMinIdle(0);
        dataSource.setMaxWait(60000);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setTestOnBorrow(false);
        dataSource.setTestWhileIdle(true);
        dataSource.setPoolPreparedStatements(false);
        return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(){
        return new JdbcTemplate(dataSource(),true);
    }
    */
}
