package cn.whaley.bi.logsys.merge;

import cn.whaley.bi.logsys.merge.entity.HiveFieldInfo;
import cn.whaley.bi.logsys.merge.entity.WhiteTabInfo;
import cn.whaley.bi.logsys.merge.service.HiveService;
import cn.whaley.bi.logsys.merge.util.GenerateDML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.collection.Iterator;
import scala.io.BufferedSource;
import scala.io.Source;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
            Integer delayDay = 0;
            if(getRelateTabNameAndFlag(productLine,realLogType) == null){
//                continue;
            }else{
                relateTabName = getRelateTabNameAndFlag(productLine,realLogType).split(",")[0].replace("__","_");
                flag = getRelateTabNameAndFlag(productLine,realLogType).split(",")[1];
                delayDay = Integer.valueOf(getRelateTabNameAndFlag(productLine,realLogType).split(",")[2]);
            }
            WhiteTabInfo whiteTabInfo = new WhiteTabInfo();
            whiteTabInfo.setTabName(tabName);
            whiteTabInfo.setProductLine(productLine);
            whiteTabInfo.setRealLogType(realLogType);
            whiteTabInfo.setLogType(logType);
            whiteTabInfo.setPathRegex(pathRegex);
            whiteTabInfo.setRelateTabName(relateTabName);
            whiteTabInfo.setFlag(flag);
            whiteTabInfo.setDelayDay(delayDay);
            whiteTabInfos.add(whiteTabInfo);

        }
/*
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
         */
        System.out.println("+===================================");
        long count = whiteTabInfos.stream().filter(whiteTabInfo -> (!whiteTabInfo.getProductLine().equals("activity") && !whiteTabInfo.getProductLine().equals("dbsnapshot"))).count();
        System.out.println("ods table size is "+count);
        System.out.println("+===================================");
        //获取ods_view tables
        List<String> tables = hiveService.getTables("ods_view", "*");
        System.out.println("ods_view table size is  "+tables.size());

        System.out.println("ods中在ods_view中 未匹配到的表");
        //ods中在ods_view中 未匹配到的表
        List<String> blackTable = new ArrayList<String>();
        blackTable.add("t_log_medusa_liv");
        blackTable.add("t_log_medusa_liveq");
        blackTable.add("t_log_medusa_plax");
        blackTable.add("t_log_whaley_helios_orcauser_errorwarning");
        blackTable.add("t_log_whaley_userduration");
        blackTable.add("t_log_medusa_kandonghua_inputmethodusage");
        blackTable.add("t_log_medusa_play_keyevent");

        blackTable.add("t_log_medusa_comic_switchinputmethod");
        blackTable.add("t_log_medusa_player_sdk_stastplay");

        whiteTabInfos.stream().filter(whiteTabInfo -> (!whiteTabInfo.getProductLine().equals("activity")
                && !whiteTabInfo.getLogType().startsWith("_")
                && !whiteTabInfo.getLogType().equals("null")
                && !blackTable.contains(whiteTabInfo.getTabName())
        )).
                forEach(whiteTabInfo -> {
                    String relateTabName = whiteTabInfo.getRelateTabName();
                    if(!tables.contains(relateTabName)){
//                        System.out.println("productLine : "+whiteTabInfo.getProductLine()+"\t tableName : "+whiteTabInfo.getTabName()+"\t logype : "+whiteTabInfo.getLogType());
                        System.out.println(whiteTabInfo.getTabName());
                    }
                });


        System.out.println("白名单表 ....");

        List<WhiteTabInfo> finalWhiteTabInfos = whiteTabInfos.stream().filter(whiteTabInfo -> (!whiteTabInfo.getProductLine().equals("activity")
                && !whiteTabInfo.getLogType().startsWith("_")
                && !whiteTabInfo.getLogType().equals("null")
                && !blackTable.contains(whiteTabInfo.getTabName())
        )).collect(Collectors.toList());



        System.out.println("白名单表 size "+finalWhiteTabInfos.size());
        finalWhiteTabInfos.forEach(whiteTabInfo -> {
            GenerateDML.generateDML(whiteTabInfo);
        });


        HashMap<String, List<String>> addColumn = new HashMap<>();
        HashMap<String, List<String>> changeColumn = new HashMap<>();

        finalWhiteTabInfos.stream()
                .filter(whiteTabInfo -> whiteTabInfo.getProductLine().equalsIgnoreCase("eagle"))
                .forEach(whiteTabInfo -> {
            String odsTabName = whiteTabInfo.getTabName();
            String productLine = whiteTabInfo.getProductLine();
            String odsViewTabName = whiteTabInfo.getRelateTabName();
            List<HiveFieldInfo> odsTableFieldInfo = hiveService.getTabFieldInfo("ods", odsTabName);
            List<HiveFieldInfo> odsViewTableFieldInfo = hiveService.getTabFieldInfo("ods_view", odsViewTabName);
            //add column,在ods table 中有在ods_view中没有，并且不是分区字段
            List<HiveFieldInfo> addColumns = odsTableFieldInfo.stream().filter(item -> {
                String colName = item.getColName();
                String dataType = item.getDataType();
                Boolean partitionField = item.getPartitionField();
                //true 有
                boolean hasField = odsViewTableFieldInfo.stream().filter(fieldInfo ->
                        fieldInfo.getColName().equalsIgnoreCase(colName) ).findAny().isPresent();
                return (hasField == false
                        && partitionField == false
                        && !"msgversion".equalsIgnoreCase(colName)
                        && !"msgsource".equalsIgnoreCase(colName)
                        && !"msgsite".equalsIgnoreCase(colName)
                        && !"msgsignflag".equalsIgnoreCase(colName)
                        && !"msgid".equalsIgnoreCase(colName)
                        && !"msgformat".equalsIgnoreCase(colName)
                        && !"http_msg_time_local".equalsIgnoreCase(colName)
                        && !"urlpath".equalsIgnoreCase(colName)
                        && !"host".equalsIgnoreCase(colName)
                        && !"hour".equalsIgnoreCase(colName)
                        && !"jsonlog".equalsIgnoreCase(colName)
                        //appid
                        && !"contenttype".equalsIgnoreCase(colName)
                        && !"day".equalsIgnoreCase(colName)
                        && !"method".equalsIgnoreCase(colName)
                        && !"receivetime".equalsIgnoreCase(colName)
                        && !"url".equalsIgnoreCase(colName)

                        && !"forwardedip".equalsIgnoreCase(colName)

                        && !"remoteip".equalsIgnoreCase(colName)
                        && !"datetime".equalsIgnoreCase(colName)
                        && !"tags".equalsIgnoreCase(colName)
                        && !"log_msgid".equalsIgnoreCase(colName)

                        //20171116排查确定的脏字段 whaley
                        && !"buffertype".equalsIgnoreCase(colName)
                        && !"exittype".equalsIgnoreCase(colName)
                        && !"sourcelist".equalsIgnoreCase(colName)
                        && !"videotime".equalsIgnoreCase(colName)
                        && !"autoswitch".equalsIgnoreCase(colName)
                        && !"definition".equalsIgnoreCase(colName)
                        && !"maxretrytimes".equalsIgnoreCase(colName)
                        && !"playertype".equalsIgnoreCase(colName)
                        && !"playurl".equalsIgnoreCase(colName)
                        && !"retrytimes".equalsIgnoreCase(colName)
                        && !"startplaysessionid".equalsIgnoreCase(colName)
                        && !"userswitch".equalsIgnoreCase(colName)
                        && !"_t".equalsIgnoreCase(colName)
                        && !"eventdate".equalsIgnoreCase(colName)
                        && !"eventhour".equalsIgnoreCase(colName)
                        && !"eventtime".equalsIgnoreCase(colName)
                        && !"forwardip".equalsIgnoreCase(colName)
                        && !"nwpingdata".equalsIgnoreCase(colName)
                        //20171117排查确定的脏字段 whaley
                        && !"phrase".equalsIgnoreCase(colName)
                        && !"errorcode".equalsIgnoreCase(colName)
                        && !"usertype".equalsIgnoreCase(colName)
                        && !"videotype".equalsIgnoreCase(colName)
                        && !"10000skip".equalsIgnoreCase(colName)
                        && !"_r".equalsIgnoreCase(colName)
                        && !"activityid".equalsIgnoreCase(colName)
                        && !"currenviptlevel".equalsIgnoreCase(colName)
                        && !"dcwpf0dc4".equalsIgnoreCase(colName)
                        && !"from".equalsIgnoreCase(colName)
                        && !"ip".equalsIgnoreCase(colName)
                        && !"scanner".equalsIgnoreCase(colName)
                        && !"set".equalsIgnoreCase(colName)
                        && !"t".equalsIgnoreCase(colName)
                        && !"sid".equalsIgnoreCase(colName)
                        //20171116排查确定的脏字段 medusa
                        && !"ap".equalsIgnoreCase(colName)
                        && !"app".equalsIgnoreCase(colName)
                        && !"appe".equalsIgnoreCase(colName)
                        && !"appen".equalsIgnoreCase(colName)
                        && !"appent".equalsIgnoreCase(colName)
                        && !"appente".equalsIgnoreCase(colName)
                        && !"appenter".equalsIgnoreCase(colName)
                        && !"we".equalsIgnoreCase(colName)
                        && !"wea".equalsIgnoreCase(colName)
                        && !"weat".equalsIgnoreCase(colName)
                        && !"weath".equalsIgnoreCase(colName)
                        && !"weathe".equalsIgnoreCase(colName)
                        && !"weather".equalsIgnoreCase(colName)
                        && !"weatherc".equalsIgnoreCase(colName)
                        && !"weatherco".equalsIgnoreCase(colName)
                        && !"weathercod".equalsIgnoreCase(colName)
                        && !"_t".equalsIgnoreCase(colName)
                        && !"pro".equalsIgnoreCase(colName)
                        && !"promotionc".equalsIgnoreCase(colName)
                        //20171117排查确定的脏字段 medusa
                        && !"ram".equalsIgnoreCase(colName)
                        && !"cpu".equalsIgnoreCase(colName)
                        && !"androidversion".equalsIgnoreCase(colName)
                        && !"contentpype".equalsIgnoreCase(colName)
                        && !"retrieva".equalsIgnoreCase(colName)
                        && !"i1m".equalsIgnoreCase(colName)
                        //20171117排查确定的脏字段 eagle
                        && !"product".equalsIgnoreCase(colName)
                        && !"dialogtype".equalsIgnoreCase(colName)
                        && !"apkvursion".equalsIgnoreCase(colName)
                        && !"condition".equalsIgnoreCase(colName)
                        && !"contentid".equalsIgnoreCase(colName)
                        && !"contentname".equalsIgnoreCase(colName)
                        && !"duration".equalsIgnoreCase(colName)
                        && !"endtype".equalsIgnoreCase(colName)
                        && !"episodesid".equalsIgnoreCase(colName)
                        && !"keyword".equalsIgnoreCase(colName)
                        && !"livchannelsid".equalsIgnoreCase(colName)
                        && !"livechanelnale".equalsIgnoreCase(colName)
                        && !"locationindex".equalsIgnoreCase(colName)
                        && !"menuinfo".equalsIgnoreCase(colName)
                        && !"nextpafeid".equalsIgnoreCase(colName)
                        && !"order".equalsIgnoreCase(colName)
                        && !"parsersessionhd".equalsIgnoreCase(colName)
                        && !"ressionid".equalsIgnoreCase(colName)
                        && !"searchresulttype".equalsIgnoreCase(colName)
                        && !"searchvideoname".equalsIgnoreCase(colName)
                        && !"searchvideosid".equalsIgnoreCase(colName)
                        && !"starttype".equalsIgnoreCase(colName)
                        && !"subcontenttypename".equalsIgnoreCase(colName)
                        && !"subtabinfo".equalsIgnoreCase(colName)
                        && !"tabinfo".equalsIgnoreCase(colName)
                        && !"tagid".equalsIgnoreCase(colName)
                        && !"videoname".equalsIgnoreCase(colName)
                        && !"videosid".equalsIgnoreCase(colName)

                        && !"buffdrtype".equalsIgnoreCase(colName)
                        && !"ctrrentpageprop".equalsIgnoreCase(colName)
                        && !"currentpagdprop".equalsIgnoreCase(colName)
                        && !"dxnamicbasicdata".equalsIgnoreCase(colName)
                        && !"dynamicbashcdata".equalsIgnoreCase(colName)
                        && !"definhtion".equalsIgnoreCase(colName)
                        && !"endthme".equalsIgnoreCase(colName)
                        && !"eventprnp".equalsIgnoreCase(colName)
                        && !"happentimd".equalsIgnoreCase(colName)
                        && !"istdst".equalsIgnoreCase(colName)
                        && !"istert".equalsIgnoreCase(colName)
                        && !"lhvechanelname".equalsIgnoreCase(colName)
                        && !"livechanelnamd".equalsIgnoreCase(colName)
                        && !"livechanndlsid".equalsIgnoreCase(colName)
                        && !"logversinn".equalsIgnoreCase(colName)
                        && !"ndxtpageid".equalsIgnoreCase(colName)
                        && !"parserressionid".equalsIgnoreCase(colName)
                        && !"parsersesrionid".equalsIgnoreCase(colName)
                        && !"playsesshonid".equalsIgnoreCase(colName)
                        && !"playsessinnid".equalsIgnoreCase(colName)
                        && !"rtarttime".equalsIgnoreCase(colName)
                        && !"sersionid".equalsIgnoreCase(colName)
                        && !"starttimd".equalsIgnoreCase(colName)
                        && !"stbcontenttype".equalsIgnoreCase(colName)
                        && !"subbontenttype".equalsIgnoreCase(colName)
                        && !"subcontenttypd".equalsIgnoreCase(colName)
                        && !"uploadtile".equalsIgnoreCase(colName)
                        && !"videotimd".equalsIgnoreCase(colName)



                );
            }).collect(Collectors.toList());

            if(addColumns.size()>0){
                //拼接ADD COLUMNS
                List<String> addColumnDDL = new ArrayList<>();
                addColumns.stream().forEach(fieldInfo->{
                    String colName = fieldInfo.getColName();
                    String ddl = String.format("ALTER TABLE `%s` ADD COLUMNS(%s)", odsViewTabName, colName);
                    addColumnDDL.add(ddl);
                });
                if(addColumnDDL.size()>0){
                    addColumn.put(odsTabName+" column size "+(odsTableFieldInfo.size()-1)+"\t|"+odsViewTabName+" column size "+(odsViewTableFieldInfo.size()-2),addColumnDDL);
                }
            }

            //change column
            List<HiveFieldInfo> changeColumns =odsTableFieldInfo.stream().filter(item -> {
                String colName = item.getColName();
                String dataType = item.getDataType();
                Boolean partitionField = item.getPartitionField();
                //true 有
                boolean hasChangedField = odsViewTableFieldInfo.stream().filter(fieldInfo ->
                        fieldInfo.getColName().equalsIgnoreCase(colName) && !fieldInfo.getDataType().equalsIgnoreCase(dataType)).findAny().isPresent();
                return (hasChangedField == true
                        && partitionField == false
                        && !"accountid".equalsIgnoreCase(colName)
//                        && !"logid".equalsIgnoreCase(colName)
//                        && !"carouselround".equalsIgnoreCase(colName)
//                        && !"relatetime".equalsIgnoreCase(colName)
//                        && !"happentime".equalsIgnoreCase(colName)
//                        && !"date_code".equalsIgnoreCase(colName)
                );
            }).collect(Collectors.toList());

            if(changeColumns.size()>0){
                //拼接change COLUMNS
                List<String> changeColumnDDL = new ArrayList<>();
                changeColumns.stream().forEach(fieldInfo->{
                    String colName = fieldInfo.getColName();
                    String newFieldType = fieldInfo.getDataType();
                    String ddl = String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s"
                            , odsViewTabName, colName, colName, newFieldType);
                    changeColumnDDL.add(ddl);
                });
                if(changeColumnDDL.size()>0){
                    changeColumn.put(odsTabName+"|"+odsViewTabName,changeColumnDDL);
                }
            }

        });

        System.out.println("新增字段 .....");
        addColumn.entrySet().stream().forEach(entity->{
            String key = entity.getKey();
            System.out.println("table is ..."+key);
            entity.getValue().forEach(ddl->{
                System.out.println(ddl);
            });
        });


        System.out.println("字段类型不一致 .....");
        changeColumn.entrySet().stream().forEach(entity->{
            String key = entity.getKey();
            System.out.println("table is ..."+key);
            entity.getValue().forEach(ddl->{
                System.out.println(ddl);
            });
        });



    }

    public static String getRelateTabNameAndFlag(String productLine,String realLogType){
        String relateTabNameAndFlag = null;
        realLogType = realLogType.toLowerCase().replace("-","_");
        switch (productLine) {
            case "medusa" :
                relateTabNameAndFlag = "log_medusa_main3x_"+realLogType+",0,1";
                break;
            case "whaley" :
                if("buffer_middle_info".equals(realLogType) || "voiceusereal".equals(realLogType)){
                    relateTabNameAndFlag = "log_whaleytv_main_"+realLogType+",0,1";
                }else{
                    relateTabNameAndFlag = "log_whaleytv_main_"+realLogType+",0,0";
                }

                break;
            case "eagle" :
                relateTabNameAndFlag = "log_eagle_main_"+realLogType+",0,0";
                break;
            case "dbsnapshot" :
                relateTabNameAndFlag = "db_snapshot_mysql_"+realLogType+",1,1";
                break;
            case "medusaAndMoretvMerger" :
                relateTabNameAndFlag = "log_medusa_merge_"+realLogType+",1,1";
                break;
            case "moretvloginlog" :
                relateTabNameAndFlag = "log_medusa_main3x_"+realLogType+",1,1";
                break;
            case "boikgpokn78sb95kjhfrendoepkseljn" :
                relateTabNameAndFlag = "log_whaleytv_global_menu_2_"+realLogType+",0,0";
                break;
            case "boikgpokn78sb95kjhfrendoj8ilnoi7" :
                relateTabNameAndFlag = "log_whaleytv_wui20_"+realLogType+",0,0";
                break;
            case "boikgpokn78sb95kjhfrendobgjgjolq" :
                relateTabNameAndFlag = "log_whaleytv_epop_"+realLogType+",0,0";
                break;
            case "boikgpokn78sb95kjhfrendojtihcg26" :
                relateTabNameAndFlag = "log_whaleytv_mobilehelper_"+realLogType+",0,0";
                break;
            case "boikgpokn78sb95kjhfrendosesh6bmu" :
                relateTabNameAndFlag = "log_whaleytv_webportal_"+realLogType+",0,0";
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
