package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.metadata.entity.LogTabDDLEntity;
import cn.whaley.bi.logsys.metadata.entity.LogTabDMLEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/16.
 */
@Repository
public class HiveRepo {

    public static Logger LOG = LoggerFactory.getLogger(HiveRepo.class);

    @Resource(name = "hiveJdbcTemplate")
    protected JdbcTemplate jdbcTemplate;

    /**
     * 查询表名(全匹配)是否存在
     *
     * @param tabName
     * @return
     */
    public Boolean tabExists(String dbName, String tabName) {
        String sql = String.format("show tables like '%s'", tabName);
        jdbcTemplate.execute("use " + dbName);
        List<String> tables = jdbcTemplate.queryForList(sql, String.class);
        return tables.contains(tabName);
    }

    /**
     * 获取特定表的字段类型信息
     *
     * @param dbName
     * @param tabName
     * @return
     */
    public List<HiveFieldInfo> getTabFieldInfo(String dbName, String tabName) {

        try {
            List<HiveFieldInfo> fieldInfos = new ArrayList<>();
            String sql = String.format("show create table `%s.%s`", dbName, tabName);
            Statement statement = jdbcTemplate.getDataSource().getConnection().createStatement();
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


    /**
     * 执行DDL语句
     *
     * @param entities
     * @return
     */
    public Integer executeDDL(List<LogTabDDLEntity> entities) {
        Integer ret = 0;
        Map<String, List<LogTabDDLEntity>> groups = entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
        for (Map.Entry<String, List<LogTabDDLEntity>> entry : groups.entrySet()) {
            String dbName = entry.getKey();
            jdbcTemplate.execute("use " + dbName);
            List<LogTabDDLEntity> group = entry.getValue();
            for (LogTabDDLEntity ddlEntity : group) {
                try {
                    ddlEntity.setCommitTime(new Date());
                    jdbcTemplate.execute(ddlEntity.getDdlText());
                    ddlEntity.setCommitCode(1);
                    ddlEntity.setCommitMsg("SUCCESS");
                    ret++;
                } catch (Exception ex) {
                    LOG.error(ddlEntity.getDdlText(), ex);
                    ddlEntity.setCommitCode(-1);
                    ddlEntity.setCommitMsg(ex.getMessage());
                }

            }
        }
        return ret;
    }

    /**
     * 执行DML语句
     *
     * @param entities
     * @return
     */
    public Integer executeDML(List<LogTabDMLEntity> entities) {
        Integer ret = 0;
        Map<String, List<LogTabDMLEntity>> groups = entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
        for (Map.Entry<String, List<LogTabDMLEntity>> entry : groups.entrySet()) {
            String dbName = entry.getKey();
            jdbcTemplate.execute("use " + dbName);
            List<LogTabDMLEntity> group = entry.getValue();
            for (LogTabDMLEntity dmlEntity : group) {
                try {
                    dmlEntity.setCommitTime(new Date());
                    jdbcTemplate.execute(dmlEntity.getDmlText());
                    dmlEntity.setCommitCode(1);
                    dmlEntity.setCommitMsg("SUCCESS");
                    ret++;
                } catch (Exception ex) {
                    LOG.error(dmlEntity.getDmlText(), ex);
                    dmlEntity.setCommitCode(-1);
                    dmlEntity.setCommitMsg(ex.getMessage());
                }
            }
        }
        return ret;
    }


}
