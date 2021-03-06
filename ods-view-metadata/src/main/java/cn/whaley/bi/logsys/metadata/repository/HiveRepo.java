package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.metadata.entity.HiveTableInfo;
import cn.whaley.bi.logsys.metadata.entity.LogTabDDLEntity;
import cn.whaley.bi.logsys.metadata.entity.LogTabDMLEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.sql.Connection;
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

    private Boolean tabExists(String dbName, String tabName, Connection conn) {
        try {
            Statement statement = conn.createStatement();
            statement.execute("use " + dbName);
            ResultSet rs = statement.executeQuery(String.format("show tables like '%s'", tabName));
            while (rs.next()) {
                //通过 hive thrift  columnIndex=1 ,spark thrift columnIndex=2
                if (rs.getString(1).equalsIgnoreCase(tabName)) {
                    return true;
                }
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    /**
     * 批量获取表字段定义
     *
     * @param dbNameAndTabNameArray dbName.tabName形式的数组
     * @return
     */
    public Map<String, HiveTableInfo> getTabInfo(List<String> dbNameAndTabNameArray) {
        Map<String, HiveTableInfo> resultMap = new HashMap<>();
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

            for (String dbNameAndTabName : dbNameAndTabNameArray) {
                HiveTableInfo tableInfo = new HiveTableInfo();
                String[] dbNameDotTabName = dbNameAndTabName.split("\\.");
                if (dbNameDotTabName.length < 2) {
                    tableInfo.setDbName("");
                    tableInfo.setTabName("");
                    tableInfo.setTabExists(false);
                } else {
                    String dbName = dbNameDotTabName[0];
                    String tabName = dbNameDotTabName[1];
                    Boolean tabExists = this.tabExists(dbName, tabName, conn);
                    tableInfo.setDbName(dbName);
                    tableInfo.setTabName(tabName);

                    tableInfo.setTabExists(tabExists);
                    if (tabExists) {
                        List<HiveFieldInfo> fieldInfos = this.getTabFieldInfo(dbNameDotTabName[0], dbNameDotTabName[1], conn);
                        tableInfo.setFieldInfos(fieldInfos);
                    }
                }
                resultMap.put(dbNameAndTabName, tableInfo);
            }
            conn.close();
            return resultMap;
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
     * spark thrift
     * @param dbName
     * @param tabName
     * @return
     */
    private List<HiveFieldInfo> getTabFieldInfo2(String dbName, String tabName, Connection conn) {

        try {
            List<HiveFieldInfo> fieldInfos = new ArrayList<>();
            String sql = String.format("show create table `%s`.`%s`", dbName, tabName);
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            String line = rs.getString(1).trim();
            String[] splits = line.split("\\(");
            //字段
            String fieldLine  = splits[1].split("\\)")[0];
            String[] fields = fieldLine.split("\\, `");
            for (int i = 0; i < fields.length; i++) {
                String[] cols = fields[i].split(" ");
                String fieldName = cols[0].trim().replace("`", "");
                String fieldType = cols[1].trim();
                HiveFieldInfo fieldInfo = new HiveFieldInfo();
                fieldInfo.setColName(fieldName);
                fieldInfo.setDataType(fieldType);
                fieldInfo.setPartitionField(false);
                fieldInfos.add(fieldInfo);
            }
            //partitions
            String[] partitions = splits[2].split("\\)")[0].split("\\, `");
            for (int i = 0; i < partitions.length; i++) {
                String[] cols = partitions[i].split(" ");
                String fieldName = cols[0].trim().replace("`", "");
                String fieldType = cols[1].trim();
                HiveFieldInfo fieldInfo = new HiveFieldInfo();
                fieldInfo.setColName(fieldName);
                fieldInfo.setDataType(fieldType);
                fieldInfo.setPartitionField(true);
                fieldInfos.add(fieldInfo);
            }
            rs.close();
            statement.close();
            return fieldInfos;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<HiveFieldInfo> getTabFieldInfo(String dbName, String tabName, Connection conn) {

        try {
            List<HiveFieldInfo> fieldInfos = new ArrayList<>();
            String sql = String.format("show create table `%s`.`%s`", dbName, tabName);
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
                                LOG.info("ddl..."+ddlEntity.getDdlText());
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
    public Integer executeDML3(List<LogTabDMLEntity> entities) {
        Integer ret = 0;
        Long fromTs = System.currentTimeMillis();
        Connection conn = null;
        Statement statement = null;
        try{
            conn = jdbcTemplate.getDataSource().getConnection();
            statement = conn.createStatement();
            Map<String, List<LogTabDMLEntity>> groups = entities.stream().collect(Collectors.groupingBy(item -> item.getDbName()));
            for (Map.Entry<String, List<LogTabDMLEntity>> entry : groups.entrySet()) {
                String dbName = entry.getKey();
                statement.execute("use " + dbName);
                List<LogTabDMLEntity> group = entry.getValue();
                //多表并行执行
                Map<String, List<LogTabDMLEntity>> tabGroups = group.stream().collect(Collectors.groupingBy(item -> item.getTabName()));
                List<Future<Integer>> futures = new ArrayList<>();
                for (Map.Entry<String, List<LogTabDMLEntity>> tabGroup : tabGroups.entrySet()) {
                    String tabName = tabGroup.getKey();
                    Collection<LogTabDMLEntity> tabDMLEntities = tabGroup.getValue().stream()
                            .sorted(Comparator.comparing(LogTabDMLEntity::getSeq))
                            .collect(Collectors.toList());
                    Long fromTs1 = System.currentTimeMillis();
                    for (LogTabDMLEntity ddlEntity : tabDMLEntities) {
                        try {
                            ddlEntity.setCommitTime(new Date());
                            statement.execute(ddlEntity.getDmlText());
                            ddlEntity.setCommitCode(1);
                            ddlEntity.setCommitMsg("SUCCESS");
                            ret++;
                        } catch (Exception ex) {
                            LOG.error(ddlEntity.getDmlText(), ex);
                            ddlEntity.setCommitCode(-1);
                            ddlEntity.setCommitMsg(ex.getMessage());
                        }
                    }
                    LOG.info("tabDML[{}.{}]: ret={}/{}, ts={}", dbName, tabName, ret, tabDMLEntities.size(), System.currentTimeMillis() - fromTs1);
                }

            }
            LOG.info("executeDML: ret={}/{},ts={}", ret, entities.size(), System.currentTimeMillis() - fromTs);
            statement.close();
            conn.close();
        }catch (Exception e){
            LOG.error("executeDML error ...");
            e.printStackTrace();
        }
        return ret;
    }


    public Integer executeDML2(List<LogTabDMLEntity> entities) {
        Integer ret = 0;
        Long fromTs = System.currentTimeMillis();

        int batchSize = 50;
        int totalSize = entities.size() ;
        if (totalSize > batchSize) {
           int times = (totalSize / batchSize) ;
           int start = 0;
            for(int i=1;i <= times ;i++){
                int end = batchSize * i -1;
                LOG.info("batch i={} times,{}->{}  " ,i,start,end);
                List<LogTabDMLEntity> logTabDMLEntities = entities.subList(start, end);
                //执行
                ret += processDML(logTabDMLEntities);
                start = batchSize * i;
            }
            int remain = totalSize - times * batchSize ;
            if (remain > 0) {
                LOG.info("batch i={} times,{}->{}  " ,times+1,times * batchSize,totalSize-1);
                List<LogTabDMLEntity> logTabDMLEntities = entities.subList(times * batchSize, totalSize-1);
                //执行
                ret += processDML(logTabDMLEntities);
            }

        }else{
           //执行
            ret += processDML(entities);
            LOG.info("batch i={} times,{}->{}  " ,1,0,totalSize-1);
        }

        LOG.info("executeDML end : ret={}/{},ts={}", ret, entities.size(), System.currentTimeMillis() - fromTs);
        return ret;
    }


    public Integer processDML(List<LogTabDMLEntity> tabDMLEntities){
        List<Future<Integer>> futures = new ArrayList<>();
        Integer ret = 0;
        Long fromTs = System.currentTimeMillis();

        Future<Integer> future = executorService.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Integer ret = 0;
                Long fromTs = System.currentTimeMillis();
                for (LogTabDMLEntity ddlEntity : tabDMLEntities) {
                    String dbName = ddlEntity.getDbName();
                    String tabName = ddlEntity.getTabName();
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
                    LOG.info("tabDML[{}.{}]: ret={}/{}, ts={}", dbName, tabName, ret, tabDMLEntities.size(), System.currentTimeMillis() - fromTs);
                }
                return ret;
            }
        });
        futures.add(future);

        Integer sumRet = futures.stream().map(fu -> {
            try {
                return fu.get();
            } catch (Exception ex) {
                LOG.error("", ex);
                return 0;
            }
        }).collect(Collectors.summingInt(item -> item));
        ret += sumRet;
        LOG.info("executeDML: ret={}/{},ts={}", ret, tabDMLEntities.size(), System.currentTimeMillis() - fromTs);
        return ret;
    }



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
