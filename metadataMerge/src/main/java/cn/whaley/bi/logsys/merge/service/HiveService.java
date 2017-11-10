package cn.whaley.bi.logsys.merge.service;

import cn.whaley.bi.logsys.merge.entity.HiveFieldInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by guohao on 2017/10/31.
 */
@Service
public class HiveService {
    public static Logger LOG = LoggerFactory.getLogger(HiveService.class);

    @Resource(name = "hiveJdbcTemplate")
    public JdbcTemplate jdbcTemplate;

    @Value("${HiveService.maxThreadPoolSize}")
    public Integer maxThreadPoolSize = 50;

    ExecutorService executorService = new ThreadPoolExecutor(10, maxThreadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    /**
     * 获取特定表的字段类型信息
     *
     * @param dbName
     * @param tabName
     * @return
     */
    public List<HiveFieldInfo> getTabFieldInfo(String dbName, String tabName) {
        try {
            Connection conn = null;
            int currTry = 0;
            while (currTry++ < 3) {
                try {
                    conn = jdbcTemplate.getDataSource().getConnection();
                    break;
                } catch (Exception ex) {
                    LOG.warn("hive error,try " + currTry);
                    if (currTry == 3) {
                        LOG.error("", ex);
                    }
                    throw ex;
                }
            }
            List<HiveFieldInfo> fieldInfos = getTabFieldInfo(dbName, tabName, conn);
            conn.close();
            return fieldInfos;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 获取特定表的字段类型信息
     *
     * @param dbName
     * @param tabName
     * @return
     */
    private List<HiveFieldInfo> getTabFieldInfo(String dbName, String tabName, Connection conn) {

        try {
            List<HiveFieldInfo> fieldInfos = new ArrayList<>();
            String sql = String.format("show create table `%s.%s`", dbName, tabName);
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            boolean isPartition = false;
            boolean isStart = false;
            while (rs.next()) {
                String line = rs.getString(1).trim();
                //忽略空行
                if (StringUtils.isEmpty(line)) {
                    continue;
                }
                if (line.startsWith("CREATE ")) {
                    isStart = true;
                    continue;
                }
                if (isStart == false) {
                    continue;
                }

                //分区行
                if (line.startsWith("PARTITIONED BY")) {
                    isPartition = true;
                    continue;
                }

                boolean isEnd = false;
                if (line.trim().endsWith(")")) {
                    isEnd = true;
                    line = line.substring(0, line.length() - 1);
                }

                if (line.trim().endsWith(")")) {
                    line = line.substring(0, line.length() - 1);
                }
                String[] cols = line.split(" ");
                String fieldName = cols[0].trim().replace("`", "");
                String fieldType = cols[1].trim();
                if (fieldType.endsWith(",")) {
                    fieldType = fieldType.substring(0, fieldType.length() - 1);
                }
                HiveFieldInfo fieldInfo = new HiveFieldInfo();
                fieldInfo.setColName(fieldName);
                fieldInfo.setDataType(fieldType);
                fieldInfo.setPartitionField(isPartition);
                fieldInfos.add(fieldInfo);

                if (isEnd && isPartition) {
                    break;
                }
            }
            rs.close();
            statement.close();
            return fieldInfos;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getTables(String dbName,String regex) throws SQLException {
       List<String> tables = new ArrayList<>();
        String sql = String.format("show tables like '%s'", regex);
        Statement statement = jdbcTemplate.getDataSource().getConnection().createStatement();
        statement.execute("use " + dbName);
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            tables.add(resultSet.getString(1));
        }
        return  tables;
    }





}
