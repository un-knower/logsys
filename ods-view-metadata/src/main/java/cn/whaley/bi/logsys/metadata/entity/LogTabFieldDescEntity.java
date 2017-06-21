package cn.whaley.bi.logsys.metadata.entity;


/**
 * Created by fj on 2017/6/14.
 */

public class LogTabFieldDescEntity extends BaseTableEntity  implements SeqEntity {

    public static final String TABLE_NAME = "metadata.logtab_field_desc";
    public static final String SEQ_NAME="metadata.seq_logtab_field_desc";

    private String dbName;
    private String tabName;
    private String fieldName;
    private Integer seq;
    private String taskId;
    private String fieldType;
    private String fieldSql;

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

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
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

    @Override
    public String getSeqName() {
        return SEQ_NAME;
    }
}
