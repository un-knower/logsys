package cn.whaley.bi.logsys.metadata.service;

import cn.whaley.bi.logsys.metadata.entity.*;
import cn.whaley.bi.logsys.metadata.repository.*;
import cn.whaley.bi.logsys.metadata.util.SendMail;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/14.
 */
@Service
public class ODSViewService {

    public static final Logger LOG = LoggerFactory.getLogger(ODSViewService.class);

    @Autowired
    LogTabFieldDescRepo logTabFieldDescRepo;

    @Autowired
    AppLogKeyFieldDescRepo appLogKeyFieldDescRepo;

    @Autowired
    LogFileFieldDescRepo logFileFieldDescRepo;

    @Autowired
    LogFileKeyFieldValueRepo logFileKeyFieldValueRepo;

    @Autowired
    LogTabDDLRepo logTabDDLRepo;

    @Autowired
    LogTabDMLRepo logTabDMLRepo;

    @Autowired
    HiveRepo hiveRepo;

    public LogTabFieldDescRepo getLogTabFieldDescRepo() {
        return logTabFieldDescRepo;
    }

    public void setLogTabFieldDescRepo(LogTabFieldDescRepo logTabFieldDescRepo) {
        this.logTabFieldDescRepo = logTabFieldDescRepo;
    }

    public AppLogKeyFieldDescRepo getAppLogKeyFieldDescRepo() {
        return appLogKeyFieldDescRepo;
    }

    public void setAppLogKeyFieldDescRepo(AppLogKeyFieldDescRepo appLogKeyFieldDescRepo) {
        this.appLogKeyFieldDescRepo = appLogKeyFieldDescRepo;
    }

    public LogFileFieldDescRepo getLogFileFieldDescRepo() {
        return logFileFieldDescRepo;
    }

    public void setLogFileFieldDescRepo(LogFileFieldDescRepo logFileFieldDescRepo) {
        this.logFileFieldDescRepo = logFileFieldDescRepo;
    }

    public LogFileKeyFieldValueRepo getLogFileKeyFieldValueRepo() {
        return logFileKeyFieldValueRepo;
    }

    public void setLogFileKeyFieldValueRepo(LogFileKeyFieldValueRepo logFileKeyFieldValueRepo) {
        this.logFileKeyFieldValueRepo = logFileKeyFieldValueRepo;
    }

    public LogTabDDLRepo getLogTabDDLRepo() {
        return logTabDDLRepo;
    }

    public void setLogTabDDLRepo(LogTabDDLRepo logTabDDLRepo) {
        this.logTabDDLRepo = logTabDDLRepo;
    }

    public LogTabDMLRepo getLogTabDMLRepo() {
        return logTabDMLRepo;
    }

    public void setLogTabDMLRepo(LogTabDMLRepo logTabDMLRepo) {
        this.logTabDMLRepo = logTabDMLRepo;
    }

    public HiveRepo getHiveRepo() {
        return hiveRepo;
    }

    public void setHiveRepo(HiveRepo hiveRepo) {
        this.hiveRepo = hiveRepo;
    }

    /**
     * 对某批任务产生对应的DDL和DML语句
     *
     * @param taskId
     * @return [ fieldRet, ddlRet, dmlRet ]
     */
    @Transactional(readOnly = false)
    public Integer[] generateDDLAndDML(String taskId) {
        List<AppLogKeyFieldDescEntity> appLogKeyFieldDescEntities = appLogKeyFieldDescRepo.findAll();
        List<LogFileKeyFieldValueEntity> logFileKeyFieldDescEntities = logFileKeyFieldValueRepo.findByTaskId(taskId);
        List<LogFileFieldDescEntity> logFileFieldDescEntities = logFileFieldDescRepo.findByTaskId(taskId);


        //扫描表字段元数据[desc ->数据库名,表名,分区字段及其值,
        // fieldDescEntities->从日志文件字段描述信息解析出表字段定义,
        // hiveTabInfo->从hive schema 中解析字段相关信息]
        List<TabFieldDescItem> tabFieldDescItems = generateTabFieldDesc(appLogKeyFieldDescEntities, logFileKeyFieldDescEntities, logFileFieldDescEntities);

        //表定义
        Map<String, HiveTableInfo> hiveTableInfoMap = hiveRepo.getTabInfo(tabFieldDescItems.stream().map(item -> item.desc.dbName + "." + item.desc.tabName).collect(Collectors.toList()));
        tabFieldDescItems.stream().forEach(new Consumer<TabFieldDescItem>() {
            @Override
            public void accept(TabFieldDescItem tabFieldDescItem) {
                tabFieldDescItem.hiveTabInfo = hiveTableInfoMap.get(tabFieldDescItem.desc.dbName + "." + tabFieldDescItem.desc.tabName);
            }
        });


        //产生DDL
        List<LogTabDDLEntity> ddlEntities = tabFieldDescItems.stream()
                .flatMap(item -> generateDDL(item).stream())
                .collect(Collectors.toList());

        //产生DML
        List<LogTabDMLEntity> dmlEntities = tabFieldDescItems.stream()
                .flatMap(item -> generateDML(item.desc).stream())
                .collect(Collectors.toList());

      /*  LOG.info("taskId:{}: field={} , ddl={} , dml={}", new Object[]{taskId
                , tabFieldDescItems.stream()
                .map(item -> item.fieldDescEntities.size())
                .collect(Collectors.summingInt(item -> item))
                , ddlEntities.size()
                , dmlEntities.size()});*/

        //保存字段描述,保存之前删除taskId对应的旧数据
        Integer fieldDelRet = getLogTabFieldDescRepo().deleteByTaskId(taskId);
        Integer fieldRet = 0;
        for (TabFieldDescItem descItem : tabFieldDescItems) {
            fieldRet += logTabFieldDescRepo.insert(descItem.fieldDescEntities);
        }

        LOG.info("taskId:{}: field.insert:{}, field.delete:{}", taskId, fieldRet, fieldDelRet);

        //保存DDL,保存之前删除taskId对应的旧数据
        Integer ddlDelRet = getLogTabDDLRepo().deleteByTaskId(taskId);
        Integer ddlRet = logTabDDLRepo.insert(ddlEntities);
        LOG.info("taskId:{}: ddl.insert:{} , ddl.delete:{}", taskId, ddlRet, ddlDelRet);


        //保存DML,保存之前删除taskId对应的旧数据
        Integer dmlDdlRet = getLogTabDMLRepo().deleteByTaskId(taskId);
        Integer dmlRet = logTabDMLRepo.insert(dmlEntities);
        LOG.info("taskId:{}: dml.insert:{} , dml.delete:{}", taskId, dmlRet, dmlDdlRet);

        return new Integer[]{fieldRet, ddlRet, dmlRet};
    }

    /**
     * 执行某批任务产生的DDL
     *
     * @param taskId
     */
    public Integer executeDDL(String taskId) {
        Integer ret = 0;
        List<LogTabDDLEntity> ddlEntities = logTabDDLRepo.queryByTaskId(taskId, false);
        if (ddlEntities.size() > 0) {
            ret += hiveRepo.executeDDL(ddlEntities);
            ddlEntities.forEach(entity -> {
                logTabDDLRepo.updateCommitInfo(entity);
            });
        }
        return ret;
    }


    /**
     * 执行某批任务产生的DML
     *
     * @param taskId
     */
    public Integer executeDML(String taskId,String deleteOld) {
        Integer ret = 0;
        List<LogTabDMLEntity> dmlEntities = logTabDMLRepo.queryForTaskId(taskId, false);
        //过滤不执行的 drop 语句
       if(!"true".equalsIgnoreCase(deleteOld.trim())){
            dmlEntities = dmlEntities.stream().filter(dmlEntity->{
                return dmlEntity.getDmlText().contains("location");
            }).collect(Collectors.toList());
        }

        if (dmlEntities.size() > 0) {
            ret += hiveRepo.executeDML(dmlEntities);
            dmlEntities.forEach(entity -> {
                logTabDMLRepo.updateCommitInfo(entity);
            });
        }
        return ret;
    }

    /**
     * 执行 select * from db.table limit 1;
     * 刷新 tblproperties
     * @param taskId
     */
    public Integer executeSql(String taskId) {
        Integer ret = 0;
        List<LogTabDDLEntity> ddlEntities = logTabDDLRepo.queryByTaskId(taskId, false);

        if(ddlEntities.size()> 0){
            List<LogTabDDLEntity> filterEntity = ddlEntities.stream().filter(entity -> {
                return !entity.getDdlText().contains("DROP TABLE");
            }).collect(Collectors.toList());

            for (int i = 0; i < filterEntity.size(); i++) {
                LogTabDDLEntity entity = filterEntity.get(i);
                String dbName = entity.getDbName();
                String tabName = entity.getTabName();
                cn.whaley.bigdata.dw.HiveUtil.generateTblProperties(dbName,tabName);
                ret=ret+1;
            }
        }
        return ret;
    }


    /**
     * 产生表字段定义描述
     *
     * @return
     */
    List<TabFieldDescItem> generateTabFieldDesc(List<AppLogKeyFieldDescEntity> appLogKeyFieldDescEntities
            , List<LogFileKeyFieldValueEntity> logFileKeyFieldDescEntities
            , List<LogFileFieldDescEntity> logFileFieldDescEntities
    ) {

        Map<String, String> logPathAppIdMap = new HashMap<>();
        logFileKeyFieldDescEntities.stream()
                .map(item -> item.getLogPath() + "," + item.getAppId())
                .distinct().forEach(item -> {
            String[] vs = item.split(",");
            logPathAppIdMap.put(vs[0], vs[1]);
        });


        List<TabFieldDescItem> results = new ArrayList<>();
        for (Map.Entry<String, String> entry : logPathAppIdMap.entrySet()) {
            String logPath = entry.getKey();
            String appId = entry.getValue();
            TabFieldDescItem result = new TabFieldDescItem();
            result.desc = resolveKeyDesc(logPath, appId, logFileKeyFieldDescEntities, appLogKeyFieldDescEntities);
            result.fieldDescEntities = resolveTabFieldDesc(result.desc, logFileFieldDescEntities);
            results.add(result);
        }
        return results;
    }


    /**
     * 产生DDL语句
     *
     * @return 产生的DDL语句条数
     */
    List<LogTabDDLEntity> generateDDL(TabFieldDescItem descItem) {

        LogFileTabKeyDesc desc = descItem.desc;
        List<LogTabFieldDescEntity> logTabFieldDescEntities = descItem.fieldDescEntities;

        //按表分组
        Map<String, List<LogTabFieldDescEntity>> tabGroups = logTabFieldDescEntities.stream()
                .collect(Collectors.groupingBy(entity -> entity.getDbName() + "." + entity.getTabName()));

        List<LogTabDDLEntity> entities = tabGroups.entrySet().stream().flatMap(entry -> {
            List<LogTabFieldDescEntity> currGroup = entry.getValue();
            return generateDDLForTab(descItem, currGroup).stream();
        }).collect(Collectors.toList());

        return entities;
    }

    /**
     * 产生DML语句
     *
     * @return 产生的DML语句条数
     */
    List<LogTabDMLEntity> generateDML(LogFileTabKeyDesc desc) {
        String partInfo = desc.getParFieldNameAndValue().stream()
                .map(par -> String.format("%s='%s'", par[0], par[1]))
                .collect(Collectors.joining(","));

        String dmlText = String.format("ALTER TABLE `%s`.`%s` DROP IF EXISTS PARTITION (%s)", desc.getDbName(), desc.getTabName(), partInfo);
        LogTabDMLEntity dropEntity = new LogTabDMLEntity();
        dropEntity.setDbName(desc.getDbName());
        dropEntity.setTabName(desc.getTabName());
        dropEntity.setDmlType("DROP PARTITION");
        dropEntity.setDmlText(dmlText);
        dropEntity.setTaskId(desc.getTaskId());

        dmlText = String.format("ALTER TABLE `%s`.`%s` ADD IF NOT EXISTS PARTITION(%s) location '%s' "
                , desc.getDbName(), desc.getTabName(), partInfo, desc.getLogPath());
        LogTabDMLEntity addEntity = new LogTabDMLEntity();
        addEntity.setDbName(desc.getDbName());
        addEntity.setTabName(desc.getTabName());
        addEntity.setDmlType("ADD PARTITION");
        addEntity.setDmlText(dmlText);
        addEntity.setTaskId(desc.getTaskId());

        return Arrays.asList(dropEntity, addEntity);
    }


    /**
     * 对某个表的一组字段定义产生相应的DDL
     *
     * @param tabGroup 某个表的一组字段定义
     * @return 产生的DDL语句条数
     */
    List<LogTabDDLEntity> generateDDLForTab(TabFieldDescItem itemDesc, List<LogTabFieldDescEntity> tabGroup) {
        List<LogTabDDLEntity> entities = new ArrayList<>();
        LogFileTabKeyDesc desc = itemDesc.desc;
        String dbName = desc.dbName;
        String tabName = desc.tabName;
        String tabFullName = dbName + "." + tabName;
        Boolean exists = itemDesc.hiveTabInfo.getTabExists();
        if (!exists) {
            //目前设计分区字段全部为string类型
            String partDesc = desc.parFieldNameAndValue.stream().map(value -> value[0] + " string").collect(Collectors.joining(","));
            String fieldDesc = StringUtils.join(tabGroup.stream()
                    .filter(entity -> {
                        //剔除分区字段
                        Boolean isPar = desc.parFieldNameAndValue.stream()
                                .filter(parItem -> parItem[0].equalsIgnoreCase(entity.getFieldName()))
                                .findAny()
                                .isPresent();
                        return isPar == false;

                    })
                    .map(entity -> entity.getFieldSql())
                    .collect(Collectors.toList()), ",");
            String ddlText = String.format("CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`%s` (%s) PARTITIONED BY (%s) STORED AS " + desc.getStored()+" location '/data_warehouse/%s.db/%s'"
                    , dbName,tabName, fieldDesc, partDesc,dbName,tabName
            );
            LogTabDDLEntity ddlEntity = new LogTabDDLEntity();
            ddlEntity.setDbName(dbName);
            ddlEntity.setTabName(tabName);
            ddlEntity.setDdlType("CREATE EXTERNAL TABLE");
            ddlEntity.setDdlText(ddlText);
            ddlEntity.setTaskId(desc.taskId);
            entities.add(ddlEntity);
        } else {

            List<HiveFieldInfo> fieldInfos = itemDesc.hiveTabInfo.getFieldInfos();
            //add column
            List<LogTabFieldDescEntity> added = tabGroup.stream().filter(item -> {
                String fieldName = item.getFieldName();
                Boolean hasField = fieldInfos.stream()
                        .filter(fieldInfo -> fieldInfo.getColName().equalsIgnoreCase(fieldName))
                        .findAny().isPresent();
                return hasField == false;
            }).collect(Collectors.toList());
            if (added.size() > 0) {
                String addColumns = added.stream().map(item -> item.getFieldSql()).collect(Collectors.joining(","));
                String ddlText = String.format("ALTER TABLE `%s`.`%s` ADD COLUMNS(%s)", dbName, tabName, addColumns);
                LogTabDDLEntity ddlEntity = new LogTabDDLEntity();
                ddlEntity.setDbName(dbName);
                ddlEntity.setTabName(tabName);
                ddlEntity.setDdlType("ADD COLUMNS");
                ddlEntity.setDdlText(ddlText);
                ddlEntity.setTaskId(desc.taskId);
                entities.add(ddlEntity);
            }

            //change column
            List<LogTabFieldDescEntity> changed = tabGroup.stream().filter(item -> {

                String fieldName = item.getFieldName();
                String fieldType = item.getFieldType();
//                System.out.println("fieldName parquet ->"+fieldName);
//                System.out.println("fieldType parquet ->"+fieldType);
                Boolean hasChangedField = fieldInfos.stream()
                        .filter(fieldInfo ->
                                fieldInfo.getColName().equalsIgnoreCase(fieldName)
                                && !fieldInfo.getDataType().equalsIgnoreCase(fieldType)
                                && !fieldInfo.getColName().equalsIgnoreCase("_msg")
                                        && !fieldInfo.getDataType().contains("struct")
                        )
                        .findAny().isPresent();
                return hasChangedField == true;
            }).collect(Collectors.toList());
            if (changed.size() > 0) {
                List<LogTabDDLEntity> changedDDLs = changed.stream().filter(change->{
                    //修复 array 类型存在的问题
                    String fieldName = change.getFieldName();
                    String newFieldType = change.getFieldSql().split(" ")[1].trim();
                    String oldFieldType = fieldInfos.stream()
                            .filter(fieldInfo -> fieldInfo.getColName().equalsIgnoreCase(fieldName) )
                            .map(fieldInfo -> fieldInfo.getDataType())
                            .findFirst().get();
                    return  !newFieldType.equalsIgnoreCase(oldFieldType) ;

                }).map(change -> {
                    String fieldName = change.getFieldName();
                    String newFieldType = change.getFieldSql().split(" ")[1].trim();
                    String oldFieldType = fieldInfos.stream()
                            .filter(fieldInfo -> fieldInfo.getColName().equalsIgnoreCase(fieldName) )
                            .map(fieldInfo -> fieldInfo.getDataType())
                            .findFirst().get();
                    String targetFieldType = newFieldType;

                   /* if (!oldFieldType.replace("`", "").replace(" ", "").equalsIgnoreCase(newFieldType.replace("`", "").replace(" ", ""))) {
                        boolean isConvertible = HiveUtil.implicitConvertible(oldFieldType, newFieldType);
                        if (!isConvertible) {
                            targetFieldType = "string";
                            LOG.info("{} : {} -> {} implicitConvertible=false, targetFieldType={}", new Object[]{fieldName, oldFieldType, newFieldType, targetFieldType});
                        }
                    }*/

                    LOG.info ("table is ->"+tabFullName +  ",fieldName ->" +fieldName + " , oldFieldType-> "+oldFieldType +", newFieldType-> "+newFieldType +" ,targetFieldType-> "+targetFieldType);
                    String ddlText = String.format("ALTER TABLE `%s`.`%s` CHANGE COLUMN `%s` `%s` %s"
                            , dbName, tabName, fieldName, fieldName, targetFieldType);
                    LOG.info ("ddlText->"+ddlText);
                    LogTabDDLEntity ddlEntity = new LogTabDDLEntity();
                    ddlEntity.setDbName(dbName);
                    ddlEntity.setTabName(tabName);
                    ddlEntity.setDdlType("CHANGE COLUMN");
                    ddlEntity.setDdlText(ddlText);
                    ddlEntity.setTaskId(desc.taskId);
                    return ddlEntity;
                }).collect(Collectors.toList());
                entities.addAll(changedDDLs);
                //取消直接执行changed语句，改为发送邮件
                //拼接执行语句
                StringBuffer context = new StringBuffer();
                changedDDLs.stream().forEach(ddlEntity->{
                    String tab = ddlEntity.getTabName();
                    String ddlText = ddlEntity.getDdlText();
                    context.append(tab+" -> "+ddlText+"\n");
                });
                //发邮件
                if(!context.toString().trim().isEmpty()){
                    String[] users = {"app-bigdata@whaley.cn"};
                    SendMail.post(context.toString(), "[ods-view-metadata][字段类型重命名]", users);
                }
            }
        }



        return entities;
    }


    //解析日志文件关键信息(数据库名,表名,分区字段及其值)
    LogFileTabKeyDesc resolveKeyDesc(String logPath, String appId, List<LogFileKeyFieldValueEntity> logFileKeyFieldDescEntities, List<AppLogKeyFieldDescEntity> appLogKeyFieldDescEntities) {

        LogFileTabKeyDesc desc = new LogFileTabKeyDesc();
        desc.setLogPath(logPath);
        desc.setTaskId(logFileKeyFieldDescEntities.get(0).getTaskId());

        List<AppLogKeyFieldDescEntity> mergedDbNameFieldDesc = mergeKeyFieldDesc(appId, AppLogKeyFieldDescEntity.FIELD_FLAG_DB_NAME, appLogKeyFieldDescEntities);
        List<AppLogKeyFieldDescEntity> mergedTabNameFieldDesc = mergeKeyFieldDesc(appId, AppLogKeyFieldDescEntity.FIELD_FLAG_TAB_NAME, appLogKeyFieldDescEntities);
        List<AppLogKeyFieldDescEntity> mergedParFieldDesc = mergeKeyFieldDesc(appId, AppLogKeyFieldDescEntity.FIELD_FLAG_PARTITION, appLogKeyFieldDescEntities);


        //日志文件关键字段值Map[fieldName,fieldValue]
        Map<String, String> fileKeyFieldDescMap = logFileKeyFieldDescEntities.stream()
                .filter(entity -> entity.getAppId().equals(appId) && entity.getLogPath().equals(logPath))
                .collect(Collectors.toMap(LogFileKeyFieldValueEntity::getFieldName, LogFileKeyFieldValueEntity::getFieldValue));

        //数据库名
        String dbName = mergedDbNameFieldDesc.stream()
                .sorted(Comparator.comparing(AppLogKeyFieldDescEntity::getFieldOrder))
                .map(entity -> {
                    //日志文件字段值->日志元数据字段预定义值
                    String fieldName = entity.getFieldName();
                    String value = fileKeyFieldDescMap.get(fieldName);
                    if (StringUtils.isEmpty(value)) {
                        value = entity.getFieldDefault();
                    }
                    return value;
                }).filter(value -> StringUtils.isNotEmpty(value)).collect(Collectors.joining("_"));
        desc.setDbName(dbName);

        //表名
        String tabName = mergedTabNameFieldDesc.stream()
                .sorted(Comparator.comparing(AppLogKeyFieldDescEntity::getFieldOrder))
                .map(entity -> {
                    //日志文件字段值->日志元数据字段预定义值
                    String fieldName = entity.getFieldName();
                    String value = fileKeyFieldDescMap.get(fieldName);
                    if (StringUtils.isEmpty(value)) {
                        value = entity.getFieldDefault();
                    }
                    return value;
                }).filter(value -> !StringUtils.isEmpty(value))
                .collect(Collectors.joining("_"));
        desc.setTabName(tabName);

        //分区字段名及其值[字段名,字段值]
        List<String[]> parFieldNameAndValue = mergedParFieldDesc.stream()
                .sorted(Comparator.comparing(AppLogKeyFieldDescEntity::getFieldOrder))
                .map(entity -> {
                    //日志文件字段值->日志元数据字段预定义值
                    String fieldName = entity.getFieldName();
                    String value = fileKeyFieldDescMap.get(fieldName);
                    if (StringUtils.isEmpty(value)) {
                        value = entity.getFieldDefault();
                    }
                    return new String[]{fieldName, value};
                }).filter(value -> StringUtils.isNotEmpty(value[1])).collect(Collectors.toList());
        desc.setParFieldNameAndValue(parFieldNameAndValue);

        return desc;
    }


    /**
     * 合并关键字段定义配置,将ALL级别和app级别进行合并,app级别有更高的优先级
     *
     * @param appId
     * @param fieldFlag
     * @param appLogKeyFieldDescEntities
     * @return 合并后的根据FieldOrder排序的关键字段定义
     */
    List<AppLogKeyFieldDescEntity> mergeKeyFieldDesc(String appId, Integer fieldFlag, List<AppLogKeyFieldDescEntity> appLogKeyFieldDescEntities) {

        //appId具体配置
        List<AppLogKeyFieldDescEntity> appEntities = appLogKeyFieldDescEntities.stream()
                .filter(entity -> entity.getAppId().equals(appId) && entity.getFieldFlag() == fieldFlag)
                .sorted(Comparator.comparing(AppLogKeyFieldDescEntity::getFieldOrder))
                .collect(Collectors.toList());

        //需要合并的默认配置,合并条件: 字段名或字段排序没有在具体appId相关配置中出现
        List<AppLogKeyFieldDescEntity> defaultEntities = appLogKeyFieldDescEntities.stream()
                .filter(entity -> entity.getAppId().equals(AppLogKeyFieldDescEntity.APP_ID_ALL)
                                && entity.getFieldFlag() == fieldFlag
                                && appEntities.stream()
                                .filter(appEntity -> appEntity.getFieldName().equals(entity.getFieldName()) || appEntity.getFieldOrder().equals(entity.getFieldOrder()))
                                .findAny().isPresent() == false
                )
                .sorted(Comparator.comparing(AppLogKeyFieldDescEntity::getFieldOrder))
                .collect(Collectors.toList());

        //将默认配置合并到appId相关配置中
        appEntities.addAll(defaultEntities);

        //在同一个appId中,不允许不同的fieldName有相同的fieldOrder
        String invalidInfo = appEntities.stream().collect(Collectors.groupingBy(entity -> entity.getFieldOrder()))
                .entrySet().stream().filter(entity -> entity.getValue().size() > 1)
                .map(entity -> entity.getKey() + ":" + entity.getValue().stream().map(value -> value.getFieldName()).collect(Collectors.joining(",")))
                .collect(Collectors.joining("\n"));

        if (StringUtils.isNotEmpty(invalidInfo)) {
            throw new IllegalStateException(invalidInfo);
        }

        //按照fieldOrder进行排序
        List<AppLogKeyFieldDescEntity> mergedEntities = appEntities.stream()
                .sorted(Comparator.comparing(AppLogKeyFieldDescEntity::getFieldOrder))
                .collect(Collectors.toList());

        return mergedEntities;

    }


    //从日志文件字段描述信息解析出表字段定义
    List<LogTabFieldDescEntity> resolveTabFieldDesc(LogFileTabKeyDesc desc, List<LogFileFieldDescEntity> fileFieldDescEntities) {
        List<LogTabFieldDescEntity> fieldDescEntities = fileFieldDescEntities.stream()
                .filter(entity -> entity.getLogPath().equals(desc.getLogPath()))
                .map(entity -> {
                    LogTabFieldDescEntity tabFieldDescEntity = new LogTabFieldDescEntity();
                    tabFieldDescEntity.setDbName(desc.getDbName());
                    tabFieldDescEntity.setTabName(desc.getTabName());
                    tabFieldDescEntity.setFieldName(entity.getFieldName());
                    tabFieldDescEntity.setSeq(null);
                    tabFieldDescEntity.setTaskId(entity.getTaskId());
                    tabFieldDescEntity.setFieldType(entity.getFieldType());
                    tabFieldDescEntity.setFieldSql(entity.getFieldSql());
                    return tabFieldDescEntity;
                })
                .collect(Collectors.toList());
        return fieldDescEntities;
    }

    //日志文件对应的表字段描述项目
    class TabFieldDescItem {
        public LogFileTabKeyDesc desc;
        public List<LogTabFieldDescEntity> fieldDescEntities;
        public HiveTableInfo hiveTabInfo;
    }

    //日志文件对应的表关键字段描述信息
    class LogFileTabKeyDesc {
        private String dbName;
        private String tabName;
        private String logPath;
        private String taskId;
        private String stored = "parquet";
        //[fieldName, fieldValue]
        private List<String[]> parFieldNameAndValue;

        private String getQualityName(String value) {
            return value.replace(".", "_").replace("-", "_").replace("__", "_");
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = getQualityName(dbName);
        }

        public String getTabName() {
            return tabName;
        }

        public void setTabName(String tabName) {
            this.tabName = getQualityName(tabName);
        }

        public String getLogPath() {
            return logPath;
        }

        public void setLogPath(String logPath) {
            this.logPath = logPath;
        }

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public String getStored() {
            return stored;
        }

        public void setStored(String stored) {
            this.stored = stored;
        }

        public List<String[]> getParFieldNameAndValue() {
            return parFieldNameAndValue;
        }

        public void setParFieldNameAndValue(List<String[]> parFieldNameAndValue) {
            this.parFieldNameAndValue = parFieldNameAndValue;
        }
    }


}
