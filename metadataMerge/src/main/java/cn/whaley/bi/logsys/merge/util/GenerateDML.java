package cn.whaley.bi.logsys.merge.util;

import cn.whaley.bi.logsys.merge.entity.WhiteTabInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wujiulin on 2017/11/13.
 */
public class GenerateDML {
    private static Logger logger = LoggerFactory.getLogger(GenerateDML.class);

    public static void main(String[] args){
        WhiteTabInfo whiteTabInfo = new WhiteTabInfo();
        whiteTabInfo.setPathRegex("/log/whaley/parquet/*/whaleyliveswitchoption");
        whiteTabInfo.setRelateTabName("log_whaleytv_main_whaleyliveswitchoption");
        whiteTabInfo.setFlag("0");
        whiteTabInfo.setDelayDay(0);
        generateDML(whiteTabInfo);
    }
    public static void generateDML(WhiteTabInfo whiteTabInfo){
        String pathRegex = whiteTabInfo.getPathRegex();
        String relateTabName = whiteTabInfo.getRelateTabName();
        String partitionSql = "";
        String resultSql = "";
        String dropSqlPrefix = "ALTER TABLE ods_view." + relateTabName + " DROP IF EXISTS " + "\n";
        String addSqlPrefix = "ALTER TABLE ods_view." + relateTabName + " ADD IF NOT EXISTS " + "\n";
        String flag = whiteTabInfo.getFlag();
        int delayDay = whiteTabInfo.getDelayDay();
        String maxDay = "20171115";
        String key_hour = "00";
        String pattern = ".*/(\\d{8})/.*";
        Pattern r = Pattern.compile(pattern);
        Configuration config = new Configuration();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            FileSystem fileSystem = FileSystem.get(config);
            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(pathRegex));
//            logger.info("total num: "+fileStatusArray.length);
            String targetPath = "/Users/wujiulin/myDocuments/DML/"+whiteTabInfo.getRelateTabName()+".sql";
            File file = new File(targetPath);
            if (!file.exists()) {
                File dir = new File(file.getParent());
                dir.mkdirs();
                file.createNewFile();
            }
            FileOutputStream outStream = new FileOutputStream(file);
            for(FileStatus fileStatus : fileStatusArray){
                String key_day = "";
                Path path = fileStatus.getPath();
                Matcher m = r.matcher(path.toString());
                if (m.find( )) {
                    key_day = m.group(1);
                } else {
                    logger.info("NO MATCH");
                }
                if(!key_day.equals("")){
                    try {
                        if(delayDay >= 1){
                            Date key_date = sdf.parse(key_day);
                            Calendar rightNow = Calendar.getInstance();
                            rightNow.setTime(key_date);
                            rightNow.add(Calendar.DAY_OF_YEAR, delayDay*-1);
                            key_day = sdf.format(rightNow.getTime());
                        }

                        if( flag.equals("0") ){
                            if(Long.valueOf(key_day) <= Long.valueOf(maxDay)){
//                                assembeDML(partitionSql,path.toString(),key_day,key_hour,outStream);
                                partitionSql = partitionSql + " PARTITION (key_day='" + key_day + "',key_hour='" + key_hour + "') LOCATION '"+path+"'" + "\n";
                            }
                        }else {
//                            assembeDML(partitionSql,path.toString(),key_day,key_hour,outStream);
                            partitionSql = partitionSql + " PARTITION (key_day='" + key_day + "',key_hour='" + key_hour + "') LOCATION '"+path+"'" + "\n";
                        }

                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
            partitionSql = addSqlPrefix + partitionSql + ";";
//            logger.info(partitionSql);
            byte[] partitionByte = partitionSql.getBytes();
            try {
                outStream.write(partitionByte);
            } catch (IOException e) {
                e.printStackTrace();
            }
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static String assembeDML(String partitionSql, String path, String key_day, String key_hour,FileOutputStream outStream){
//        String dropPartition = "ALTER TABLE ods_view." + tableName + " DROP IF EXISTS PARTITION (key_day='" + key_day + "');" + "\n";
//        String addPartition = "ALTER TABLE ods_view." + tableName + " ADD IF NOT EXISTS PARTITION (key_day='" + key_day + "',key_hour='" + key_hour + "') LOCATION '"+path+"';" + "\n";
//        partitionSql = partitionSql + " PARTITION (key_day='" + key_day + "',key_hour='" + key_hour + "') LOCATION '"+path+"'" + "\n";
//        logger.info(partitionSql);
//        return partitionSql;
//        logger.info("log path: "+path);
//        logger.info("key_day: " + key_day);
//        logger.info("dropPartition: "+dropPartition);
//        logger.info("addPartition: "+addPartition);
//        byte[] dropPartitionByte = dropPartition.getBytes();
//        byte[] addPartitionByte = addPartition.getBytes();
//        try {
//            outStream.write(dropPartitionByte);
//            outStream.write(addPartitionByte);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}
