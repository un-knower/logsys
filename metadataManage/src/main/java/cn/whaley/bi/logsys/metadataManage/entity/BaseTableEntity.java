package cn.whaley.bi.logsys.metadataManage.entity;

import java.util.Date;

/**
 * Created by fj on 2017/6/14.
 */

public abstract class BaseTableEntity {

    boolean isDeleted = false;
    Date createTime = new Date();
    Date updateTime = new Date();

    public abstract String getUnderlyingTabName();

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
