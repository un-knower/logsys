package cn.whaley.bi.logsys.metadataManage.entity;


/**
 * Created by fj on 2017/6/14.
 */

public class AppLogKeyFieldDescEntity extends BaseTableEntity{

    public static final String TABLE_NAME = "metadata.applog_key_field_desc";
    public static final Integer FIELD_FLAG_DB_NAME = 0;
    public static final Integer FIELD_FLAG_TAB_NAME = 1;
    public static final Integer FIELD_FLAG_PARTITION = 2;
    //适用于所有AppId的公用配置项目
    public static final String APP_ID_ALL="ALL";

    private String appId;
    private String fieldName;
    private Integer fieldFlag;
    private Integer fieldOrder;
    private String fieldDefault;

    @Override
    public String getUnderlyingTabName(){
        return TABLE_NAME;
    }

    public static String getTableName() {
        return TABLE_NAME;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getFieldFlag() {
        return fieldFlag;
    }

    public void setFieldFlag(Integer fieldFlag) {
        this.fieldFlag = fieldFlag;
    }

    public Integer getFieldOrder() {
        return fieldOrder;
    }

    public void setFieldOrder(Integer fieldOrder) {
        this.fieldOrder = fieldOrder;
    }

    public String getFieldDefault() {
        return fieldDefault;
    }

    public void setFieldDefault(String fieldDefault) {
        this.fieldDefault = fieldDefault;
    }


}
