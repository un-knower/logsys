package cn.whaley.bi.logsys.metadata.entity;

import java.util.List;

/**
 * Created by fj on 2017/7/11.
 */
public class HiveTableInfo {
    private String dbName;
    private String tabName;
    private Boolean tabExists;
    private List<HiveFieldInfo> fieldInfos;

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

    public Boolean getTabExists() {
        return tabExists;
    }

    public void setTabExists(Boolean tabExists) {
        this.tabExists = tabExists;
    }

    public List<HiveFieldInfo> getFieldInfos() {
        return fieldInfos;
    }

    public void setFieldInfos(List<HiveFieldInfo> fieldInfos) {
        this.fieldInfos = fieldInfos;
    }
}
