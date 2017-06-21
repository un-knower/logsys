package cn.whaley.bi.logsys.metadata.entity;


/**
 * Created by fj on 2017/6/14.
 */

public class LogFileFieldDescEntity extends BaseTableEntity {

    public static final String TABLE_NAME = "metadata.logfile_field_desc";

    private String logPath;
    private String fieldName;
    private String fieldType;
    private String fieldSql;
    private String rawType;
    private String rawInfo;
    private String taskId;

    @Override
    public String getUnderlyingTabName(){
        return TABLE_NAME;
    }

    public static String getTableName() {
        return TABLE_NAME;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
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

    public String getFieldSql() {
        return fieldSql;
    }

    public void setFieldSql(String fieldSql) {
        this.fieldSql = fieldSql;
    }

    public String getRawType() {
        return rawType;
    }

    public void setRawType(String rawType) {
        this.rawType = rawType;
    }

    public String getRawInfo() {
        return rawInfo;
    }

    public void setRawInfo(String rawInfo) {
        this.rawInfo = rawInfo;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
