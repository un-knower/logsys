package cn.whaley.bi.logsys.metadata.entity;

/**
 * Created by guohao on 2017/11/22.
 * 用于元数据管理过滤黑名单的表 的信息
 */
public class BlackTableInfoEntity extends BaseTableEntity {
    public static final String TABLE_NAME = "metadata.black_table_info";
    private String id ;
    private String tableName; // 黑名单表
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
