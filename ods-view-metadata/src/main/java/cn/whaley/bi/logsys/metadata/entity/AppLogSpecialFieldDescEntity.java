package cn.whaley.bi.logsys.metadata.entity;


/**
 * Created by fj on 2017/6/14.
 */

public class AppLogSpecialFieldDescEntity extends BaseTableEntity   {

    public static final String TABLE_NAME = "metadata.applog_special_field_desc";

    private String id ;
    private String tabNameReg ;
    private String logPathReg ;
    private String fieldNameReg ;
    private String specialType ;
    private String  specialValue ;
    private Integer specialOrder ;



    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTabNameReg() {
        return tabNameReg;
    }

    public void setTabNameReg(String tabNameReg) {
        this.tabNameReg = tabNameReg;
    }

    public String getLogPathReg() {
        return logPathReg;
    }

    public void setLogPathReg(String logPathReg) {
        this.logPathReg = logPathReg;
    }

    public String getFieldNameReg() {
        return fieldNameReg;
    }

    public void setFieldNameReg(String fieldNameReg) {
        this.fieldNameReg = fieldNameReg;
    }

    public String getSpecialType() {
        return specialType;
    }

    public void setSpecialType(String specialType) {
        this.specialType = specialType;
    }

    public String getSpecialValue() {
        return specialValue;
    }

    public void setSpecialValue(String specialValue) {
        this.specialValue = specialValue;
    }

    public Integer getSpecialOrder() {
        return specialOrder;
    }

    public void setSpecialOrder(Integer specialOrder) {
        this.specialOrder = specialOrder;
    }

    @Override
    public String getUnderlyingTabName() {
        return this.TABLE_NAME;
    }
}
