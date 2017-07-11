package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.metadata.entity.LogTabDDLEntity;
import cn.whaley.bi.logsys.metadata.entity.LogTabDMLEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/16.
 */
@Repository
public class HiveRepo {

    public static Logger LOG = LoggerFactory.getLogger(HiveRepo.class);

    @Resource(name = "hiveJdbcTemplate")
    public JdbcTemplate jdbcTemplate;

    @Value("${HiveRepo.maxThreadPoolSize}")
    public Integer maxThreadPoolSize = 50;

    ExecutorService executorService = new ThreadPoolExecutor(10, maxThreadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

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
        Long fromTs = System.currentTimeMillis();
        Map<String, List<LogTabDDLEntity>> groups = entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
        for (Map.Entry<String, List<LogTabDDLEntity>> entry : groups.entrySet()) {
            String dbName = entry.getKey();
            jdbcTemplate.execute("use " + dbName);
            List<LogTabDDLEntity> group = entry.getValue();
            //多表并行执行
            Map<String, List<LogTabDDLEntity>> tabGroups = group.stream().collect(Collectors.groupingBy(item -> item.getTabName()));
            List<Future<Integer>> futures = new ArrayList<>();
            for (Map.Entry<String, List<LogTabDDLEntity>> tabGroup : tabGroups.entrySet()) {
                String tabName = tabGroup.getKey();
                Collection<LogTabDDLEntity> tabDDLEntities = tabGroup.getValue().stream()
                        .sorted(Comparator.comparing(LogTabDDLEntity::getSeq))
                        .collect(Collectors.toList());
                Future<Integer> future = executorService.submit(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        Integer ret = 0;
                        Long fromTs = System.currentTimeMillis();
                        for (LogTabDDLEntity ddlEntity : tabDDLEntities) {
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
                        LOG.info("tabDDL[{}.{}]: ret={}/{}, ts={}", dbName, tabName, ret, tabDDLEntities.size(), System.currentTimeMillis() - fromTs);
                        return ret;
                    }
                });
                futures.add(future);
            }

            Integer sumRet = futures.stream().map(future -> {
                try {
                    return future.get();
                } catch (Exception ex) {
                    LOG.error("", ex);
                    return 0;
                }
            }).collect(Collectors.summingInt(item -> item));

            ret += sumRet;
        }
        LOG.info("executeDDL: ret={}/{},ts={}", ret, entities.size(), System.currentTimeMillis() - fromTs);
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
        Long fromTs = System.currentTimeMillis();
        Map<String, List<LogTabDMLEntity>> groups = entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
        for (Map.Entry<String, List<LogTabDMLEntity>> entry : groups.entrySet()) {
            String dbName = entry.getKey();
            jdbcTemplate.execute("use " + dbName);
            List<LogTabDMLEntity> group = entry.getValue();
            //多表并行执行
            Map<String, List<LogTabDMLEntity>> tabGroups = group.stream().collect(Collectors.groupingBy(item -> item.getTabName()));
            List<Future<Integer>> futures = new ArrayList<>();
            for (Map.Entry<String, List<LogTabDMLEntity>> tabGroup : tabGroups.entrySet()) {
                String tabName = tabGroup.getKey();
                Collection<LogTabDMLEntity> tabDMLEntities = tabGroup.getValue().stream()
                        .sorted(Comparator.comparing(LogTabDMLEntity::getSeq))
                        .collect(Collectors.toList());

                Future<Integer> future = executorService.submit(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        Integer ret = 0;
                        Long fromTs = System.currentTimeMillis();
                        for (LogTabDMLEntity ddlEntity : tabDMLEntities) {
                            try {
                                ddlEntity.setCommitTime(new Date());
                                jdbcTemplate.execute(ddlEntity.getDmlText());
                                ddlEntity.setCommitCode(1);
                                ddlEntity.setCommitMsg("SUCCESS");
                                ret++;
                            } catch (Exception ex) {
                                LOG.error(ddlEntity.getDmlText(), ex);
                                ddlEntity.setCommitCode(-1);
                                ddlEntity.setCommitMsg(ex.getMessage());
                            }
                        }
                        LOG.info("tabDML[{}.{}]: ret={}/{}, ts={}", dbName, tabName, ret, tabDMLEntities.size(), System.currentTimeMillis() - fromTs);
                        return ret;
                    }
                });
                futures.add(future);
            }

            Integer sumRet = futures.stream().map(future -> {
                try {
                    return future.get();
                } catch (Exception ex) {
                    LOG.error("", ex);
                    return 0;
                }
            }).collect(Collectors.summingInt(item -> item));

            ret += sumRet;
        }
        LOG.info("executeDML: ret={}/{},ts={}", ret, entities.size(), System.currentTimeMillis() - fromTs);
        return ret;
    }


}
