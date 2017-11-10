package cn.whaley.bi.logsys.merge;

import cn.whaley.bi.logsys.merge.entity.WhiteTabInfo;
import cn.whaley.bi.logsys.merge.service.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.collection.Iterator;
import scala.io.BufferedSource;
import scala.io.Source;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by guohao on 2017/10/31.
 */
public class Start {
    public static final Logger LOG = LoggerFactory.getLogger(Start.class);
    public static void main(String[] args) throws SQLException {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-bean.xml");
        HiveService hiveService = context.getBean(HiveService.class);

        String path = Thread.currentThread().getContextClassLoader().getResource("whiteTabInfo.txt").getPath();
        BufferedSource bufferedSource = Source.fromFile(path, "UTF-8");
        ArrayList<WhiteTabInfo> whiteTabInfos = new ArrayList<>();
        Iterator<String> lines = bufferedSource.getLines();
        while (lines.hasNext()){
            String[] splits = lines.next().split("\\|");
            String tabName = splits[0];
            String productLine = splits[1];
            String realLogType = splits[2];
            String logType = splits[3];
            String pathRegex = "" ;
            if(getLogPathFromProductLine(productLine) == null){
//                continue;
            }else{
                pathRegex = getLogPathFromProductLine(productLine)+realLogType ;
            }
            String relateTabName = "" ;
            String flag = "";
            if(getRelateTabNameAndFlag(productLine,realLogType) == null){
//                continue;
            }else{
                relateTabName = getRelateTabNameAndFlag(productLine,realLogType).split(",")[0].replace("__","_");
                flag = getRelateTabNameAndFlag(productLine,realLogType).split(",")[1];
            }
            WhiteTabInfo whiteTabInfo = new WhiteTabInfo();
            whiteTabInfo.setTabName(tabName);
            whiteTabInfo.setProductLine(productLine);
            whiteTabInfo.setRealLogType(realLogType);
            whiteTabInfo.setLogType(logType);
            whiteTabInfo.setPathRegex(pathRegex);
            whiteTabInfo.setRelateTabName(relateTabName);
            whiteTabInfo.setFlag(flag);
            whiteTabInfos.add(whiteTabInfo);

        }
        whiteTabInfos.stream().forEach(whiteTabInfo -> {
            System.out.println(whiteTabInfo.toString());
        });

        System.out.println("+===================================");
        whiteTabInfos.stream().filter(whiteTabInfo -> (whiteTabInfo.getPathRegex()=="" && !whiteTabInfo.getProductLine().equals("activity") && !whiteTabInfo.getProductLine().equals("dbsnapshot")))
                .forEach(whiteTabInfo -> {
            System.out.println(whiteTabInfo.toString());
        });

        System.out.println("+===================================");
        whiteTabInfos.stream().filter(whiteTabInfo ->  (whiteTabInfo.getRelateTabName()=="" && !whiteTabInfo.getProductLine().equals("activity") && !whiteTabInfo.getProductLine().equals("dbsnapshot")))
                .forEach(whiteTabInfo -> {
                    System.out.println(whiteTabInfo.toString());
                });
        System.out.println("+===================================");
        long count = whiteTabInfos.stream().filter(whiteTabInfo -> (!whiteTabInfo.getProductLine().equals("activity") && !whiteTabInfo.getProductLine().equals("dbsnapshot"))).count();
        System.out.println("ods table size is "+count);
        System.out.println("+===================================");
        //获取ods_view tables
        List<String> tables = hiveService.getTables("ods_view", "*");
        System.out.println("ods_view table size is  "+tables.size());



    /*    tables.forEach(table ->{
            System.out.println("ods_view table is "+table);
        });*/


        System.out.println("ods中在ods_view中 未匹配到的表");
        //ods中在ods_view中 未匹配到的表
        whiteTabInfos.stream().filter(whiteTabInfo -> (!whiteTabInfo.getProductLine().equals("activity") && !whiteTabInfo.getProductLine().equals("dbsnapshot"))).
                forEach(whiteTabInfo -> {
                    String relateTabName = whiteTabInfo.getRelateTabName();
                    if(!tables.contains(relateTabName)){
//                        String tabName = whiteTabInfo.getTabName();
                        System.out.println(whiteTabInfo.toString());
                    }
                });


    }

    public static String getRelateTabNameAndFlag(String productLine,String realLogType){
        String relateTabNameAndFlag = null;
        realLogType = realLogType.toLowerCase().replace("-","_");
        switch (productLine) {
            case "medusa" :
                relateTabNameAndFlag = "log_medusa_main3x_"+realLogType+",0";
                break;
            case "whaley" :
                relateTabNameAndFlag = "log_whaleytv_main_"+realLogType+",0";
                break;
            case "eagle" :
                relateTabNameAndFlag = "log_eagle_main_"+realLogType+",0";
                break;
            case "dbsnapshot" :
                relateTabNameAndFlag = "db_snapshot_mysql_"+realLogType+",1";
                break;
            case "medusaAndMoretvMerger" :
                relateTabNameAndFlag = "log_medusa_merge_"+realLogType+",1";
                break;
            case "moretvloginlog" :
                relateTabNameAndFlag = "log_medusa_main3x_"+realLogType+",1";
                break;
            case "boikgpokn78sb95kjhfrendoepkseljn" :
                relateTabNameAndFlag = "log_whaleytv_global_menu_2_"+realLogType+",0";
                break;
            case "boikgpokn78sb95kjhfrendoj8ilnoi7" :
                relateTabNameAndFlag = "log_whaleytv_wui20_"+realLogType+",0";
                break;
            case "boikgpokn78sb95kjhfrendobgjgjolq" :
                relateTabNameAndFlag = "log_whaleytv_epop_"+realLogType+",0";
                break;
            case "boikgpokn78sb95kjhfrendojtihcg26" :
                relateTabNameAndFlag = "log_whaleytv_mobilehelper_"+realLogType+",0";
                break;
            case "boikgpokn78sb95kjhfrendosesh6bmu" :
                relateTabNameAndFlag = "log_whaleytv_webportal_"+realLogType+",0";
                break;
            default: relateTabNameAndFlag = null;
        }
        return relateTabNameAndFlag ;
    }


    public static String getLogPathFromProductLine(String productLine){
        String logPath = null;
        switch (productLine) {
            case "medusa" :
                logPath = "/log/medusa/parquet/*/";
                break;
            case "whaley" :
                logPath = "/log/whaley/parquet/*/";
                break;
            case "dbsnapshot" :
                logPath = "/log/dbsnapshot/parquet/*/";
                break;
            case "medusaAndMoretvMerger" :
                logPath = "/log/medusaAndMoretvMerger/*/";
                break;
            case "moretvloginlog" :
                logPath = "/log/moretvloginlog/parquet/*/";
                break;
            case "eagle" :
                logPath = "/log/eagle/parquet/*/";
                break;
//            case "activity" :
//                logPath = "/log/activity/parquet/*/";
//                break;
            case "boikgpokn78sb95kjhfrendoepkseljn" :
                logPath = "/log/boikgpokn78sb95kjhfrendoepkseljn/parquet/*/";
                break;
            case "boikgpokn78sb95kjhfrendoj8ilnoi7" :
                logPath = "/log/boikgpokn78sb95kjhfrendoj8ilnoi7/parquet/*/";
                break;
            case "boikgpokn78sb95kjhfrendobgjgjolq" :
                logPath = "/log/boikgpokn78sb95kjhfrendobgjgjolq/parquet/*/";
                break;
            case "boikgpokn78sb95kjhfrendojtihcg26" :
                logPath = "/log/boikgpokn78sb95kjhfrendojtihcg26/parquet/*/";
                break;
            case "boikgpokn78sb95kjhfrendosesh6bmu" :
                logPath = "/log/boikgpokn78sb95kjhfrendosesh6bmu/parquet/*/";
                break;
             default: logPath = null;
        }
        return logPath ;
    }


}
