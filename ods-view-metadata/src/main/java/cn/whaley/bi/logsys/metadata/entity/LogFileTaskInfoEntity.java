package cn.whaley.bi.logsys.metadata.entity;


/**
 * Created by fj on 2017/6/14.
 */

public class LogFileTaskInfoEntity extends BaseTableEntity {

    public static final String TABLE_NAME = "metdata.logfile_task_info";

    private String taskId;
    private String taskType;
    private String taskCode;
    private String taskMsg;
    private String timeFrom;
    private String timeTo;
    private String logSrcPath;
    private String logTargetPaths;

    @Override
    public String getUnderlyingTabName(){
        return TABLE_NAME;
    }

    public static String getTableName() {
        return TABLE_NAME;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getTaskCode() {
        return taskCode;
    }

    public void setTaskCode(String taskCode) {
        this.taskCode = taskCode;
    }

    public String getTaskMsg() {
        return taskMsg;
    }

    public void setTaskMsg(String taskMsg) {
        this.taskMsg = taskMsg;
    }

    public String getTimeFrom() {
        return timeFrom;
    }

    public void setTimeFrom(String timeFrom) {
        this.timeFrom = timeFrom;
    }

    public String getTimeTo() {
        return timeTo;
    }

    public void setTimeTo(String timeTo) {
        this.timeTo = timeTo;
    }

    public String getLogSrcPath() {
        return logSrcPath;
    }

    public void setLogSrcPath(String logSrcPath) {
        this.logSrcPath = logSrcPath;
    }

    public String getLogTargetPaths() {
        return logTargetPaths;
    }

    public void setLogTargetPaths(String logTargetPaths) {
        this.logTargetPaths = logTargetPaths;
    }
}
