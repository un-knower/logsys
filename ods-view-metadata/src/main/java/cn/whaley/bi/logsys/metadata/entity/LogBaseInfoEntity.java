package cn.whaley.bi.logsys.metadata.entity;

/**
 * Created by guohao on 2017/10/18.
 * log baseinfo 的信息
 */
public class LogBaseInfoEntity extends BaseTableEntity {
    public static final String TABLE_NAME = "metadata.log_baseinfo";
    private String id ;
    private String productCode;
    private String productCodeId;
    private String fieldName;
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

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getProductCodeId() {
        return productCodeId;
    }

    public void setProductCodeId(String productCodeId) {
        this.productCodeId = productCodeId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
