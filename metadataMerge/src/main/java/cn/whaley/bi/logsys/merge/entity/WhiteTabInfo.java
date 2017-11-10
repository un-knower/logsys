package cn.whaley.bi.logsys.merge.entity;

/**
 * Created by guohao on 2017/11/1.
 */
public class WhiteTabInfo {
    private String tabName ; //ods.db 中的表名称
    private String productLine;
    private String realLogType ; //真实的logType
    private String logType ; //若logType以medusa开头，去掉medusa
    private String pathRegex ; //路径正则，时间用*代替
    private String relateTabName ; //ods_view.db 中的表名称
    private String flag ; //0分区到11.1,   1：分区到当天

    public String getTabName() {
        return tabName;
    }

    public void setTabName(String tabName) {
        this.tabName = tabName;
    }

    public String getProductLine() {
        return productLine;
    }

    public void setProductLine(String productLine) {
        this.productLine = productLine;
    }

    public String getRealLogType() {
        return realLogType;
    }

    public void setRealLogType(String realLogType) {
        this.realLogType = realLogType;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getPathRegex() {
        return pathRegex;
    }

    public void setPathRegex(String pathRegex) {
        this.pathRegex = pathRegex;
    }

    public String getRelateTabName() {
        return relateTabName;
    }

    public void setRelateTabName(String relateTabName) {
        this.relateTabName = relateTabName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "WhiteTabInfo{" +
                "tabName='" + tabName + '\'' +
                ", productLine='" + productLine + '\'' +
                ", realLogType='" + realLogType + '\'' +
                ", logType='" + logType + '\'' +
                ", pathRegex='" + pathRegex + '\'' +
                ", relateTabName='" + relateTabName + '\'' +
                ", flag='" + flag + '\'' +
                '}';
    }
}
