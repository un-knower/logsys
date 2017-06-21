package cn.whaley.bi.logsys.metadata.entity;


/**
 * Created by fj on 2017/6/14.
 */

public class LogFileKeyFieldDescEntity extends BaseTableEntity {

    public static final String TABLE_NAME = "metadata.logfile_key_field_value";

    private String logPath;
    private String fieldName;
    private String fieldValue;
    private String appId;
    private String taskId;

    @Override
    public String getUnderlyingTabName(){
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

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
