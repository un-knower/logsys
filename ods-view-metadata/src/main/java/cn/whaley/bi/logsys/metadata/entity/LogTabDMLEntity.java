package cn.whaley.bi.logsys.metadata.entity;


import java.util.Date;

/**
 * Created by fj on 2017/6/14.
 */

public class LogTabDMLEntity extends BaseTableEntity   implements SeqEntity{

    public static final String TABLE_NAME = "metadata.logtab_dml";
    public static final String SEQ_NAME="metadata.seq_logtab_dml";

    private String dbName ;
    private String tabName;
    private Integer seq  ;
    private String taskId ;
    private String dmlType ;// --DML类型, ADD_PARTITION
    private String dmlText ;// --DML语句
    private Date commitTime ;// --DDL提交时间
    private Integer commitCode ;// --DDL提交结果
    private String commitMsg ;// --DDL提交结果说明

    @Override
    public String getUnderlyingTabName(){
        return TABLE_NAME;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTabName() {
        return tabName;
    }

    public void setTabName(String tabName) {
        this.tabName = tabName;
    }

    public Integer getSeq() {
        return seq;
    }

    public void setSeq(Integer seq) {
        this.seq = seq;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getDmlType() {
        return dmlType;
    }

    public void setDmlType(String dmlType) {
        this.dmlType = dmlType;
    }

    public String getDmlText() {
        return dmlText;
    }

    public void setDmlText(String dmlText) {
        this.dmlText = dmlText;
    }

    public Date getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(Date commitTime) {
        this.commitTime = commitTime;
    }

    public Integer getCommitCode() {
        return commitCode;
    }

    public void setCommitCode(Integer commitCode) {
        this.commitCode = commitCode;
    }

    public String getCommitMsg() {
        return commitMsg;
    }

    public void setCommitMsg(String commitMsg) {
        this.commitMsg = commitMsg;
    }

    @Override
    public String getSeqName() {
        return SEQ_NAME;
    }
}
