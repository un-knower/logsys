package cn.whaley.bi.logsys.metadata.entity;

/**
 * Created by guohao on 2017/11/22.
 * log 字段类型 的信息
 */
public class LogFieldTypeInfoEntity extends BaseTableEntity {
    public static final String TABLE_NAME = "metadata.log_field_type_info";
    private String id ;
    private String tableName ; //表名称
    private String realLogType ; //日志类型名称
    private String fieldName ; //字段
    private String fieldType; //字段类型
    private String typeFlag;  //字段类型标识 1:String 2:Long 3:Double
    private String ruleType ; //规则类型 1.table:表规则 2.realLogType:日志类型级别 3.field:字段级别
    @Override
    public String getUnderlyingTabName() {
        return TABLE_NAME;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRealLogType() {
        return realLogType;
    }

    public void setRealLogType(String realLogType) {
        this.realLogType = realLogType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getTypeFlag() {
        return typeFlag;
    }

    public void setTypeFlag(String typeFlag) {
        this.typeFlag = typeFlag;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }
}
