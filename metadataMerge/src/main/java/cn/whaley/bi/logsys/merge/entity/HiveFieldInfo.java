package cn.whaley.bi.logsys.merge.entity;

/**
 * Created by guohao on 2017/10/31.
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
