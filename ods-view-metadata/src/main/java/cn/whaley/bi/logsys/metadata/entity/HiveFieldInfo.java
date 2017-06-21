package cn.whaley.bi.logsys.metadata.entity;

/**
 * Created by fj on 2017/6/16.
 */
public class HiveFieldInfo {
    private String colName;
    private String dataType;
    private Boolean partitionField;

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Boolean getPartitionField() {
        return partitionField;
    }

    public void setPartitionField(Boolean partitionField) {
        this.partitionField = partitionField;
    }
}
