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
            //jdbcTemplate的封装方法目前不支持describe语句,所以需要直接调用底层驱动
            List<HiveFieldInfo> fieldInfos = new ArrayList<>();
            String sql = String.format("describe %s.%s", dbName, tabName);
            Statement statement = jdbcTemplate.getDataSource().getConnection().createStatement();
            ResultSet rs = statement.executeQuery(sql);
            boolean isPartition = false;
            while (rs.next()) {
                String f1 = rs.getString(1);
                String f2 = rs.getString(2);
                //忽略空行
                if (StringUtils.isAnyEmpty(f1, f2)) {
                    continue;
                }
                //分区行
                if (f1.startsWith("# col_name")) {
                    isPartition = true;
                    continue;
                }

                if (isPartition) {
                    fieldInfos.stream()
                            .filter(fieldInfo -> fieldInfo.getColName().equals(f1))
                            .forEach(item -> item.setPartitionField(true));
                } else {
                    HiveFieldInfo fieldInfo = new HiveFieldInfo();
                    fieldInfo.setColName(f1);
                    fieldInfo.setDataType(f2);
                    fieldInfo.setPartitionField(false);
                    fieldInfos.add(fieldInfo);
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
    public void executeDDL(List<LogTabDDLEntity> entities) {
        Map<String, List<LogTabDDLEntity>> groups =  entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
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
                } catch (Exception ex) {
                    LOG.error(ddlEntity.getDdlText(), ex);
                    ddlEntity.setCommitCode(-1);
                    ddlEntity.setCommitMsg(ex.getMessage());
                }

            }
        }
    }

    /**
     * 执行DML语句
     *
     * @param entities
     * @return
     */
    public void executeDML(List<LogTabDMLEntity> entities) {
        Map<String, List<LogTabDMLEntity>> groups =  entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
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
                } catch (Exception ex) {
                    LOG.error(dmlEntity.getDmlText(), ex);
                    dmlEntity.setCommitCode(-1);
                    dmlEntity.setCommitMsg(ex.getMessage());
                }
            }
        }
    }


}
