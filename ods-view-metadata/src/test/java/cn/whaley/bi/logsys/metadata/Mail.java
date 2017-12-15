package cn.whaley.bi.logsys.metadata;

import cn.whaley.bi.logsys.metadata.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.metadata.util.SendMail;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Created by guohao on 2017/11/17.
 */
public class Mail {
    @Test
    public void test(){
        String[] users = {"app-bigdata@whaley.cn"};
        SendMail.post("test1111","test",users);
    }

    @Test
    public void test1(){
        String str = "CREATE EXTERNAL TABLE `ods_view`.`log_medusa_main3x_homeaccess`(`_msg` struct<logSignFlag:bigint,msgFormat:string,msgId:string,msgSignFlag:bigint,msgSite:string,msgSource:string,msgVersion:string>, `accessArea` string, `accessLocation` string, `accountId` bigint, `aid` string, `alg` string, `androidVersion` string, `apkSeries` string, `apkVersion` string, `appEnterWay` string, `appId` string, `biz` string, `buildDate` string, `cityLevel` string, `contentCode` string, `contentName` string, `contentType` string, `countId` string, `cpu` string, `date` string, `datetime` string, `day` string, `deviceId` string, `dns` string, `event` string, `groupId` string, `ip` string, `ipsFromIP` string, `ipsFromKAIP` string, `keepAliveIP` string, `landingPage` string, `linkType` string, `liveEntranceType` string, `liveType` string, `locationIndex` string, `logId` string, `logTime` bigint, `logTimestamp` bigint, `logType` string, `logVersion` string, `mac` string, `md5` string, `networkType` string, `positionContentType` string, `productModel` string, `promotionChannel` string, `ram_r` string, `realIP` string, `realLogType` string, `recommendType` string, `relateTime` string, `sessionId` string, `sourceId` string, `ssid` string, `svr_content_type` string, `svr_forwarded_for` string, `svr_host` string, `svr_receive_time` bigint, `svr_remote_addr` string, `svr_req_method` string, `svr_req_url` string, `ts` bigint, `uploadTime` string, `userId` string, `userType` string, `version` string, `versionCode` string, `weatherCode` string, `wifiMac` string) PARTITIONED BY (`key_day` string, `key_hour` string) a";
        String[] splits = str.split("\\(");
        String fieldLine  = splits[1].split("\\)")[0];
        System.out.println(fieldLine);
        String[] fields = fieldLine.split("\\, `");
        for (int i = 0; i < fields.length; i++) {
            String[] cols = fields[i].split(" ");
            String fieldName = cols[0].trim().replace("`", "");
            String fieldType = cols[1].trim();
            System.out.println("fieldName ->"+fieldName +": fieldType->"+fieldType);
            HiveFieldInfo fieldInfo = new HiveFieldInfo();
            fieldInfo.setColName(fieldName);
            fieldInfo.setDataType(fieldType);
            fieldInfo.setPartitionField(false);
        }


        System.out.println("-------------------------");

        String[] partitions = splits[2].split("\\)")[0].split("\\, `");
        for (int i = 0; i < partitions.length; i++) {
            String[] cols = partitions[i].split(" ");
            String fieldName = cols[0].trim().replace("`", "");
            String fieldType = cols[1].trim();
            System.out.println("fieldName ->"+fieldName +": fieldType->"+fieldType);
            HiveFieldInfo fieldInfo = new HiveFieldInfo();
            fieldInfo.setColName(fieldName);
            fieldInfo.setDataType(fieldType);
            fieldInfo.setPartitionField(true);
        }

    }
}
