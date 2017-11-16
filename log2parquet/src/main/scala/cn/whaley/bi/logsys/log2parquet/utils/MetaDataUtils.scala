package cn.whaley.bi.logsys.log2parquet.utils

import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.constant.LogKeys
import cn.whaley.bi.logsys.metadata.entity.AppLogKeyFieldDescEntity
import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


case class MetaDataUtils(metadataServer: String, readTimeOut: Int = 100000) {

    def metadataService() = new MetadataService(metadataServer, readTimeOut)

    /**
     * 查询字段特例表
      *
      * @return Seq[(logPathReg,fieldNameReg,specialType,specialValue,specialOrder)]
     */
    def queryAppLogSpecialFieldDescConf(): Seq[(String, String, String, String, Int)] = {
        val items = metadataService.getAllAppLogSpecialFieldDesc().toList.filter(item => item.isDeleted == false)
        items.map(item => {
            (item.getLogPathReg(), item.getFieldNameReg, item.getSpecialType, item.getSpecialValue, item.getSpecialOrder.toInt)
        })
    }

    /**
     * 解析合并了ALL全局配置的字段配置
      *
      * @param fieldFlag
     * @return Map[appId, List[(appId, fieldName, fieldDefault, fieldOrder)]
     */
    def resolveAppLogKeyFieldDescConfig(fieldFlag: Int): Map[String, List[(String, String, String, Int)]] = {
        val items = metadataService.getAllAppLogKeyFieldDesc()
            .filter(item => item.getFieldFlag == fieldFlag && item.isDeleted == false)
        resolveAppLogKeyFieldDescConfig(items)
    }

    /**
     * 解析某一类合并了ALL全局配置的字段配置
      *
      * @param confs 某一类型fieldFlag的配置项
     * @return Map[appId, List[(appId, fieldName, fieldDefault, fieldOrder)]
     */
    def resolveAppLogKeyFieldDescConfig(confs: Seq[AppLogKeyFieldDescEntity]): Map[String, List[(String, String, String, Int)]] = {
        val rs = confs.map(row => (row.getAppId, row.getFieldName, row.getFieldDefault, row.getFieldOrder.toInt))
        rs.map(row => row._1).distinct.map(appId => {
          //同一顺序只允许一个字段名
          //合并,最多两行,ALL行和appId行
          val appFields = rs.filter(row => row._1 == "ALL" || row._1 == appId)
          val fields = appFields.groupBy(row => row._4).map(group => {
            assert(group._2.map(item => item._2).distinct.size == 1)
            if (group._2.size == 1 || group._2.head._1 != "ALL") {
              group._2(0)
            } else {
              group._2(1)
            }
          }).toList.sortBy(row => row._4)
          (appId, fields)
        }).toMap
    }


    /**
     * 对RDD的每条记录解析其输出路径,格式错误的行将被忽略
      *
     * @return
     */
 /*   def parseLogStrRddPath(rdd: RDD[String])(implicit accumulator:LongAccumulator=rdd.sparkContext.longAccumulator): RDD[(String, JSONObject, scala.collection.mutable.Map[String,String])] = {
        val jsonObjRdd = rdd.map(row => {
            try {
                Some(JSON.parseObject(row))
            }
            catch {
                case _: Throwable => {
                    None
                }
            }
        }).filter(row => row.isDefined).map(row => row.get)
        parseLogObjRddPath(jsonObjRdd)
    }*/

    /**
     * 对RDD的每条记录解析其输出路径
      *
      * @param rdd
     * @return
     */
    def parseLogObjRddPath(rdd: RDD[JSONObject],key_day:String,key_hour:String)
                          (implicit myAccumulator:MyAccumulator=new MyAccumulator
                          ): RDD[(String, JSONObject,scala.collection.mutable.Map[String,String])] = {
        val dbNameFieldMap = resolveAppLogKeyFieldDescConfig(0)
        val tabNameFieldMap = resolveAppLogKeyFieldDescConfig(1)
        val parFieldMap = resolveAppLogKeyFieldDescConfig(2)

        rdd.map(jsonObj => parseLogObjPath(myAccumulator,jsonObj, dbNameFieldMap, tabNameFieldMap, parFieldMap,key_day,key_hour)).filter(rdd=>rdd._1 !=null)
    }

  /*  def parseLogObjRddPathTest(rdd: RDD[JSONObject]): RDD[(String, JSONObject,scala.collection.mutable.Map[String,String])] = {
        /*  val dbNameFieldMap = resolveAppLogKeyFieldDescConfig(0)
          val tabNameFieldMap = resolveAppLogKeyFieldDescConfig(1)
          val parFieldMap = resolveAppLogKeyFieldDescConfig(2)*/

        val d = Map("ALL" -> List(("ALL", "db_name", "ods_view", 0)))
        val t = Map("boikgpokn78sb95ktmsc1bnkechpgj9l" -> List(("ALL", "tab_prefix", "log", 0), ("boikgpokn78sb95ktmsc1bnkechpgj9l", "product_code", "medusa", 1), ("boikgpokn78sb95ktmsc1bnkechpgj9l", "app_code", "main3x", 2), ("ALL", "logType", null, 3), ("ALL", "eventId", null, 4)))
        val partitionMap = Map("ALL" -> List(("ALL", "key_day", null, 0), ("ALL", "key_hour", null, 1)))
        rdd.map(jsonObj => parseLogObjPath(jsonObj, d, t, partitionMap))
    }*/

    /**
     * 解析某条日志记录的输出路径
      *
      * @param logObj
     * @return
     */
    def parseLogObjPath(implicit myAccumulator:MyAccumulator=new MyAccumulator,
                        logObj: JSONObject
                        , dbNameFieldMap: Map[String, List[(String, String, String, Int)]]
                        , tabNameFieldMap: Map[String, List[(String, String, String, Int)]]
                        , parFieldMap: Map[String, List[(String, String, String, Int)]]
                        ,key_day:String,key_hour:String): (String,JSONObject, scala.collection.mutable.Map[String,String]) = {
        val appId = logObj.getString(LogKeys.LOG_APP_ID)
        var dbNameFields = dbNameFieldMap.get(appId)
        var tabNameFields = tabNameFieldMap.get(appId)
        var parFields = parFieldMap.get(appId)

        if (dbNameFields.isEmpty) dbNameFields = dbNameFieldMap.get("ALL")
        if (tabNameFields.isEmpty) tabNameFields = tabNameFieldMap.get("ALL")
        if (parFields.isEmpty) parFields = parFieldMap.get("ALL")

        val dbTuple=getOrDefault(0, logObj, dbNameFields,key_day,key_hour)
        val dbNameStr =  dbTuple._1
        val dbMap =  dbTuple._2

        val tableTuple = getOrDefault(1, logObj, tabNameFields,key_day,key_hour)
        val tabNameStr =tableTuple._1
        val tableMap =tableTuple._2

        val parTuple = getOrDefault(2, logObj, parFields,key_day,key_hour)
        val parStr = parTuple._1
        val parMap = parTuple._2
        var path = (tabNameStr :: parStr :: Nil).filter(item => item != "").mkString("/").replace("-", "_").replace(".", "")
        if (dbNameStr != "") path = dbNameStr.replace("-", "_").replace(".", "") + ".db/" + path

//        println(s"dbNameStr -> $dbNameStr ; tabNameStr -> $tabNameStr ; parStr-> $parStr")
//      println(s"path -> $path")
        if(!isValid(parStr) || !isValid(tabNameStr) || !isValid(dbNameStr) ){
            myAccumulator.add("exceptionJsonAcc")
            path = null
        }
        (path, logObj,dbMap++tableMap++parMap+(LogKeys.LOG_APP_ID->appId))
    }

    //优先级: jsonObj字段值 -> conf字段值 , 如果两者都为空,则忽略该字段
    def getOrDefault(fieldFlag: Int, jsonObj: JSONObject,
                     conf: Option[List[(String, String, String, Int)]],
                     key_day:String,key_hour:String): (String,scala.collection.mutable.HashMap[String,String]) = {
        if (conf.isDefined) {

//          val logBody = jsonObj //jsonObj.getJSONObject("logBody")
          //特殊处理 在没有logType，只有logtype的情况下，将logtype重命名为logType
          if(jsonObj.containsKey("logtype") && !jsonObj.containsKey(LogKeys.LOG_TYPE)){
            val logType = jsonObj.get("logtype")
              jsonObj.put(LogKeys.LOG_TYPE,logType)
              jsonObj.remove("logtype")
          }

            //处理table 路径生成规则
            val logType = jsonObj.getString(LogKeys.LOG_TYPE)
            var realLogType = if(LogKeys.EVENT.equals(logType)){
                jsonObj.getString(LogKeys.LOG_BODY_EVENT_ID)
            }else if(LogKeys.START_END.equals(logType)){
                jsonObj.getString(LogKeys.LOG_BODY_ACTION_ID)
            }else{
                logType
            }

            /*if(realLogType == null){
                realLogType = "default"
            }*/
            val appId = jsonObj.getString("appId")
            appId match {
                case LogKeys.WUI_20_APPID =>{
                    //修复wui20,未打logType字段
                    if(realLogType == null){
                        realLogType = jsonObj.getString("eventId")
                        jsonObj.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,realLogType)
                        jsonObj.put(LogKeys.LOG_TYPE,"event")

                    }
                }
                case LogKeys.EAGLE_APPID =>{
                    //修复eagle,未打logType字段
                    if(realLogType == null){
                        realLogType = jsonObj.getString("eventId")
                      //修复eagle打点play日志应该为live
                        if("play".equals(realLogType)){
                          realLogType = "live"
                        }
                        jsonObj.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,realLogType)
                        jsonObj.put(LogKeys.LOG_BODY_EVENT_ID,realLogType)
                        jsonObj.put(LogKeys.LOG_TYPE,"event")
                    }
                }
                case LogKeys.EPOP_APPID =>{
                    //修复epop  线下店演示用的应用，作用是 保证所有电视播的画面是同步的
                    if(realLogType == null){
                        realLogType = jsonObj.getString("eventId")
                        jsonObj.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,realLogType)
                        jsonObj.put(LogKeys.LOG_TYPE,"event")
                    }
                }
                case LogKeys.GLOBAL_MENU_2_APPID =>{
                    //修复global_menu_2 全局菜单2.0
                    if(realLogType == null){
                        realLogType = jsonObj.getString("eventId")
                        jsonObj.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,realLogType)
                        jsonObj.put(LogKeys.LOG_TYPE,"event")
                    }
                }
                case LogKeys.MOBILEHELPER_APPID =>{
                    //修复mobilehelper 微鲸手机助手
                    if(realLogType == null){
                        realLogType = jsonObj.getString("eventId")
                        jsonObj.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,realLogType)
                        jsonObj.put(LogKeys.LOG_TYPE,"event")
                    }
                }
                case _ => ""
            }

           /* if(realLogType == null){
              realLogType = "default"
            }*/
           if(isInValidLogType(realLogType)){
             return  ("",scala.collection.mutable.HashMap.empty[String,String])
           }
            jsonObj.put(LogKeys.LOG_BODY_REAL_LOG_TYPE,realLogType.toLowerCase)
            val fields = conf.get.map(field => {
                val fieldName = field._2
                var fieldValue = field._3
                val fieldOrder = field._4
                /*if (fieldName == "key_day" && !logBody.containsKey("key_day")) {
                    val logTime = new Date()
                    logTime.setTime(jsonObj.getLongValue("logTime"))
                    fieldValue = new SimpleDateFormat("yyyyMMdd").format(logTime)
                } else if (fieldName == "key_hour" && !logBody.containsKey("key_hour")) {
                    val logTime = new Date()
                    logTime.setTime(jsonObj.getLongValue("logTime"))
                    fieldValue = new SimpleDateFormat("HH").format(logTime)
                }*/

                if (fieldName == LogKeys.LOG_KEY_DAY) {
                    fieldValue = key_day
                } else if (fieldName == LogKeys.LOG_KEY_HOUR) {
                    fieldValue = key_hour
                }


                if (jsonObj.containsKey(fieldName)
                    && jsonObj.get(fieldName) != null
                    && jsonObj.get(fieldName).toString.trim.length > 0) {
                    fieldValue = jsonObj.get(fieldName).toString
                }
                if (fieldValue != null && fieldValue.trim.length > 0) {
                    Some((fieldName, fieldValue, fieldOrder))
                } else {
                    None
                }
            }).filter(item => item.isDefined).map(item => item.get).sortBy(item => item._3)


            val map = scala.collection.mutable.HashMap.empty[String,String]
            if (fieldFlag == 0 || fieldFlag == 1) {
                val name=fields.map(item => {
                  map.+=(item._1->item._2)
                  item._2
                }).mkString("_")
              (name,map)
            } else {
                val name=fields.map(item => {
                  map.+=(item._1->item._2)
                  s"${item._1}=${item._2}"
                }).mkString("/")
              (name,map)
            }
        } else {
          ("",scala.collection.mutable.HashMap.empty[String,String])
        }
    }
    /**
     * 解析字段特例规则库
      *
      * @param rdd
     * @return Map[logPath,(字段黑名单,字段重命名清单,行过滤器)]
     */
    def parseSpecialRules(rdd: RDD[(String, JSONObject)]): Array[AppLogFieldSpecialRules] = {



        //特例字段配置数据
        val specialFieldDescConf = queryAppLogSpecialFieldDescConf
        //路径及其所有字段集合
        val pathAndFields = rdd.map(row => {
            val line = row._2
            line.remove(LogKeys.LOG_MSG_MSG)
            (row._1, line.keySet().toArray(new Array[String](0)))
        }).
          reduceByKey((set1, set2) => {
            val set = set1.filter(item => set2.contains(item) == false)
            if (!set.isEmpty) {
                set ++ set2
            } else {
                set2
            }
        }).collect()


      //baseinfo 白名单
      val logBaseInfos = metadataService().getAllLogBaseInfo().filter(item=>{
        item.isDeleted == false
      }).map(item=>{
        (item.getProductCode,item.getFieldName)
      })
      val baseInfoMap = logBaseInfos.groupBy(item=>{
        item._1
      }).map(info=>{
        val list = new ListBuffer[String]()
        info._2.foreach(f=>{
          list +=(f._2)
        })
        (info._1,list.toList)
      })

        val rules = pathAndFields.map(item => {
            val path = item._1
            val productCode = path.split("/")(1).split("_")(1)
            val fields = item._2
            //匹配当前路径的配置,且排序值最小的一组配置
            var pathSpecialConf = specialFieldDescConf.filter(conf => conf._1.r.findFirstMatchIn(path).isDefined)
            //println(s"${path}: all specialConf.length=${pathSpecialConf.length},pathFields.length=${fields.length}")
            if (!pathSpecialConf.isEmpty) {
                //排序值最小的一组配置
                val order = pathSpecialConf.minBy(conf => conf._5)
                pathSpecialConf = pathSpecialConf.filter(conf => conf._5 == order._5)
                //println(s"${path}: actual specialConf.length=${pathSpecialConf.length}")

                //字段过滤器: Seq[源字段名]
                val fieldFilterList = pathSpecialConf.filter(conf => conf._3 == "fieldFilter").flatMap(conf => {
                    val specialValue = conf._4
                    val fieldPattern = if (specialValue.charAt(0) == '1') {
                        Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE)
                    }
                    else {
                        Pattern.compile(conf._2)
                    }
                    val isReserve = specialValue.charAt(1) == '0'
                    //not used (Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE), isReserve)
                    fields.filter(field => fieldPattern.matcher(field).find()).map(field => (field, isReserve))
                })

                //剔除白名单字段 和 字母 ，纯数字
                val whiteList = fieldFilterList.filter(_._2)
                val fieldBlackFilter = fieldFilterList.filter(item => {
                  val field = item._1
                  !whiteList.exists(p => p._1 == field)
                }).map(item => item._1)


                //字段重命名: Seq[(源字段名,字段目标名)]
                val rename = pathSpecialConf.filter(conf => conf._3 == "rename").flatMap(conf => {
                    fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
                })

               //baseInfo 重命名
               val baseInfoFields = baseInfoMap.getOrElse(productCode,List[String](""))
              val baseInfoRename = ArrayBuffer[(String, String)]()
              baseInfoFields.foreach(baseInfoField=>{
                fields.foreach(field=>{
                  if(!baseInfoField.equals(field) && baseInfoField.equalsIgnoreCase(field)){
                    baseInfoRename.+=((field,s"${field}_r"))
                  }
                })
              })
                //行过滤器: Seq[(字段名,字段值)]
                val rowBlackFilter = pathSpecialConf.filter(conf => conf._3 == "rowFilter").flatMap(conf => {
                    fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
                })
                AppLogFieldSpecialRules(path, fieldBlackFilter, rename,baseInfoRename,rowBlackFilter)
            } else {
                AppLogFieldSpecialRules(path, Array[String](), Array[(String, String)](),Array[(String, String)](), Array[(String, String)]())
            }
        })

        rules

    }

    case class AppLogFieldSpecialRules(path: String, fieldBlackFilter: Seq[String], rename: Seq[(String, String)],baseInfoRename: Seq[(String, String)], rowBlackFilter: Seq[(String, String)])

  /**
    * dbNameStr,tabNameStr,parStr 正则
    * @param s
    * @return
    */
    def isValid(s:String)={
        val regex = """[a-zA-Z0-9-_=/\.]+"""
        s.matches(regex)
    }

  /**
    * json key 的正则
    * @param s
    * @return
    */
  def isinValidKey(s:String)={
    val regex = "^[0-9]*$"
    s.matches(regex)
  }

  /**
    * realLogType正则
    * @param realLogType
    * @return
    */
 def isInValidLogType(realLogType:String)={
   val regex = "^[a-zA-Z][\\w\\-]{1,100}$"
   realLogType == null || realLogType.isEmpty  || realLogType.equalsIgnoreCase("null") || !realLogType.matches(regex)
  }
}
