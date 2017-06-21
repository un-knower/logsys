package cn.whaley.bi.logsys.metadata.entity;


import cn.whaley.bi.logsys.metadata.repository.SeqGenerator;

import java.util.Date;

/**
 * Created by fj on 2017/6/14.
 */

public class LogTabDDLEntity extends BaseTableEntity  implements SeqEntity {

    public static final String TABLE_NAME = "metadata.logtab_ddl";
    public static final String SEQ_NAME = "metadata.seq_logtab_ddl";

    private String dbName;
    private String tabName;
    private Integer seq;
    private String taskId;
    private String ddlType;//--DDL类型, CHANGE_COLUMN/ADD_COLUMN/DROP_COLUMN/CREATE_TABLE/ALTER_TABLE/DROP_TABLE
    private String ddlText;
    private Date commitTime = new Date();
    private Integer commitCode = 0;
    private String commitMsg = "";

    @Override
    public String getUnderlyingTabName() {
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

    public String getDdlType() {
        return ddlType;
    }

    public void setDdlType(String ddlType) {
        this.ddlType = ddlType;
    }

    public String getDdlText() {
        return ddlText;
    }

    public void setDdlText(String ddlText) {
        this.ddlText = ddlText;
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
