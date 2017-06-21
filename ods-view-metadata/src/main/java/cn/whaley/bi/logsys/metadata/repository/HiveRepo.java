package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.metadata.entity.LogTabDDLEntity;
import cn.whaley.bi.logsys.metadata.entity.LogTabDMLEntity;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.jdo.annotations.Transactional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/16.
 */
@Repository
public class HiveRepo {

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
        List<HiveFieldInfo> fieldInfos = new ArrayList<HiveFieldInfo>();
        String sql = String.format("desc %s.%s", dbName, tabName);
        SqlRowSet rs = jdbcTemplate.queryForRowSet(sql);
        boolean start1 = false;
        boolean start2 = false;
        while (rs.next()) {
            String f1 = rs.getString(1);
            String f2 = rs.getString(2);
            if (StringUtils.isAnyEmpty(f1, f2)) {
                continue;
            }
            if ("col_name".equals(f1) && "data_type".equals(f2)) {
                start1 = true;
                continue;
            }
            if (f1.startsWith("#") && f1.indexOf("Partition Information") > 0) {
                start2 = true;
                continue;
            }
            if (f1.startsWith("#") || start1 == false) {
                continue;
            }

            HiveFieldInfo fieldInfo = new HiveFieldInfo();
            fieldInfo.setColName(f1);
            fieldInfo.setDataType(f2);
            fieldInfo.setPartitionField(start2);
            fieldInfos.add(fieldInfo);
        }
        return fieldInfos;
    }

    /**
     * 执行DDL语句
     *
     * @param entity
     * @return
     */
    public void executeDDL(List<LogTabDDLEntity> entity) {
        int ret = 0;
        Map<String, List<LogTabDDLEntity>> groups = entity.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
        for (Map.Entry<String, List<LogTabDDLEntity>> entry : groups.entrySet()) {
            String dbName = entry.getKey();
            jdbcTemplate.execute("use " + dbName);
            List<LogTabDDLEntity> entities = entry.getValue();
            for (LogTabDDLEntity ddlEntity : entities) {
                jdbcTemplate.execute(ddlEntity.getDdlText());
            }
        }
        ;
    }

    /**
     * 执行DML语句
     *
     * @param entity
     * @return
     */
    public void executeDML(List<LogTabDMLEntity> entity) {
        Map<String, List<LogTabDMLEntity>> groups = entity.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
        for (Map.Entry<String, List<LogTabDMLEntity>> entry : groups.entrySet()) {
            String dbName = entry.getKey();
            jdbcTemplate.execute("use " + dbName);
            List<LogTabDMLEntity> entities = entry.getValue();
            for (LogTabDMLEntity dmlEntity : entities) {
                jdbcTemplate.execute(dmlEntity.getDmlText());
            }
        }
    }


}
