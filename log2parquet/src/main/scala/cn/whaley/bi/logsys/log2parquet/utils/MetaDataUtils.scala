package cn.whaley.bi.logsys.log2parquet.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import cn.whaley.bi.logsys.metadata.entity.AppLogKeyFieldDescEntity
import com.alibaba.fastjson.{JSONObject, JSON}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

case class MetaDataUtils(metadataServer: String, readTimeOut: Int = 100000) {

    def metadataService() = new MetadataService(metadataServer, readTimeOut)

    /**
     * 查询字段特例表
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
     * @param rdd
     * @return
     */
    def parseLogStrRddPath(rdd: RDD[String]): RDD[(String, JSONObject)] = {
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
    }

    /**
     * 对RDD的每条记录解析其输出路径
     * @param rdd
     * @return
     */
    def parseLogObjRddPath(rdd: RDD[JSONObject]): RDD[(String, JSONObject)] = {
        val dbNameFieldMap = resolveAppLogKeyFieldDescConfig(0)
        val tabNameFieldMap = resolveAppLogKeyFieldDescConfig(1)
        val parFieldMap = resolveAppLogKeyFieldDescConfig(2)
        rdd.map(jsonObj => parseLogObjPath(jsonObj, dbNameFieldMap, tabNameFieldMap, parFieldMap))
    }

    def parseLogObjRddPathTest(rdd: RDD[JSONObject]): RDD[(String, JSONObject)] = {
        /*  val dbNameFieldMap = resolveAppLogKeyFieldDescConfig(0)
          val tabNameFieldMap = resolveAppLogKeyFieldDescConfig(1)
          val parFieldMap = resolveAppLogKeyFieldDescConfig(2)*/

        val d = Map("ALL" -> List(("ALL", "db_name", "ods_view", 0)))
        val t = Map("boikgpokn78sb95ktmsc1bnkechpgj9l" -> List(("ALL", "tab_prefix", "log", 0), ("boikgpokn78sb95ktmsc1bnkechpgj9l", "product_code", "medusa", 1), ("boikgpokn78sb95ktmsc1bnkechpgj9l", "app_code", "main3x", 2), ("ALL", "logType", null, 3), ("ALL", "eventId", null, 4)))
        val partitionMap = Map("ALL" -> List(("ALL", "key_day", null, 0), ("ALL", "key_hour", null, 1)))
        rdd.map(jsonObj => parseLogObjPath(jsonObj, d, t, partitionMap))
    }

    /**
     * 解析某条日志记录的输出路径
     * @param logObj
     * @return
     */
    def parseLogObjPath(logObj: JSONObject
                        , dbNameFieldMap: Map[String, List[(String, String, String, Int)]]
                        , tabNameFieldMap: Map[String, List[(String, String, String, Int)]]
                        , parFieldMap: Map[String, List[(String, String, String, Int)]]
                           ): (String, JSONObject) = {
        val appId = logObj.getString("appId")
        var dbNameFields = dbNameFieldMap.get(appId)
        var tabNameFields = tabNameFieldMap.get(appId)
        var parFields = parFieldMap.get(appId)

        if (dbNameFields.isEmpty) dbNameFields = dbNameFieldMap.get("ALL")
        if (tabNameFields.isEmpty) tabNameFields = tabNameFieldMap.get("ALL")
        if (parFields.isEmpty) parFields = parFieldMap.get("ALL")

        val dbNameStr = getOrDefault(0, logObj, dbNameFields)
        val tabNameStr = getOrDefault(1, logObj, tabNameFields)
        val parStr = getOrDefault(2, logObj, parFields)

        var path = (tabNameStr :: parStr :: Nil).filter(item => item != "").mkString("/").replace("-", "_").replace(".", "")
        if (dbNameStr != "") path = dbNameStr.replace("-", "_").replace(".", "") + ".db/" + path
        (path, logObj)

    }

    //优先级: jsonObj字段值 -> conf字段值 , 如果两者都为空,则忽略该字段
    def getOrDefault(fieldFlag: Int, jsonObj: JSONObject, conf: Option[List[(String, String, String, Int)]]): String = {
        if (conf.isDefined) {
            val logBody = jsonObj.getJSONObject("logBody")
            val fields = conf.get.map(field => {
                val fieldName = field._2
                var fieldValue = field._3
                val fieldOrder = field._4

                if (fieldName == "key_day" && !logBody.containsKey("key_day")) {
                    val logTime = new Date()
                    logTime.setTime(jsonObj.getLongValue("logTime"))
                    fieldValue = new SimpleDateFormat("yyyyMMdd").format(logTime)
                } else if (fieldName == "key_hour" && !logBody.containsKey("key_hour")) {
                    val logTime = new Date()
                    logTime.setTime(jsonObj.getLongValue("logTime"))
                    fieldValue = new SimpleDateFormat("HH").format(logTime)
                }

                if (logBody.containsKey(fieldName)
                    && logBody.get(fieldName) != null
                    && logBody.get(fieldName).toString.trim.length > 0) {
                    fieldValue = logBody.get(fieldName).toString
                }
                if (fieldValue != null && fieldValue.trim.length > 0) {
                    Some((fieldName, fieldValue, fieldOrder))
                } else {
                    None
                }
            }).filter(item => item.isDefined).map(item => item.get).sortBy(item => item._3)
            if (fieldFlag == 0 || fieldFlag == 1) {
                fields.map(item => item._2).mkString("_")
            } else {
                fields.map(item => s"${item._1}=${item._2}").mkString("/")
            }
        } else {
            ""
        }
    }


    /**
     * 解析字段特例规则库
     * @param rdd
     * @return Map[logPath,(字段黑名单,字段重命名清单,行过滤器)]
     */
    def parseSpecialRules(rdd: RDD[(String, JSONObject)]): Array[AppLogFieldSpecialRules] = {

        //特例字段配置数据
        val specialFieldDescConf = queryAppLogSpecialFieldDescConf

        //路径及其所有字段集合
        val pathAndFields = rdd.map(row => (row._1, row._2.getJSONObject("logBody").keySet().toArray(new Array[String](0)))).reduceByKey((set1, set2) => {
            val set = set1.filter(item => set2.contains(item) == false)
            if (!set.isEmpty) {
                set ++ set2
            } else {
                set2
            }
        }).collect()

        val rules = pathAndFields.map(item => {
            val path = item._1
            val fields = item._2

            //匹配当前路径的配置,且排序值最小的一组配置
            var pathSpecialConf = specialFieldDescConf.filter(conf => conf._1.r.findFirstMatchIn(path).isDefined)
            println(s"${path}: all specialConf.length=${pathSpecialConf.length},pathFields.length=${fields.length}")
            if (!pathSpecialConf.isEmpty) {
                //排序值最小的一组配置
                val order = pathSpecialConf.minBy(conf => conf._5)
                pathSpecialConf = pathSpecialConf.filter(conf => conf._5 == order._5)
                println(s"${path}: actual specialConf.length=${pathSpecialConf.length}")

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
                    (Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE), isReserve)
                    fields.filter(field => fieldPattern.matcher(field).find()).map(field => (field, isReserve))
                })

                //剔除白名单字段
                val whiteList = fieldFilterList.filter(item => item._2 == true)
                val fieldBlackFilter = fieldFilterList.filter(item => whiteList.exists(p => p._1 == item._1) == false).map(item => item._1)


                //字段重命名: Seq[(源字段名,字段目标名)]
                val rename = pathSpecialConf.filter(conf => conf._3 == "rename").flatMap(conf => {
                    fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
                })

                //行过滤器: Seq[(字段名,字段值)]
                val rowBlackFilter = pathSpecialConf.filter(conf => conf._3 == "rowBlackFilter").flatMap(conf => {
                    fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
                })
                AppLogFieldSpecialRules(path, fieldBlackFilter, rename, rowBlackFilter)
            } else {
                AppLogFieldSpecialRules(path, Array[String](), Array[(String, String)](), Array[(String, String)]())
            }
        })

        rules

    }

    case class AppLogFieldSpecialRules(path: String, fieldBlackFilter: Seq[String], rename: Seq[(String, String)], rowBlackFilter: Seq[(String, String)])

}
