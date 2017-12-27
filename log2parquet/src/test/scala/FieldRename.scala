import java.util.regex.Pattern

import cn.whaley.bi.logsys.log2parquet.utils.{MetaDataUtils, SendMail}
import cn.whaley.bi.logsys.metadata.entity.LogBaseInfoEntity

import scala.collection.mutable

/**
  * Created by guohao on 2017/12/23.
  */
object FieldRename {
  def main(args: Array[String]): Unit = {


    var fields = List("locationIndex_r","locationIndex","accessLocation","accesslocation")
    val productCode = "whaleytv"
    val tableName = "log_whaleytv_main_launcher"

    val metaDataUtils = new MetaDataUtils("http://odsviewmd.whaleybigdata.com", 100000)
    val appLogSecialEntities = metaDataUtils.queryAppLogSpecialFieldDescConf()
    val baseInfoEntities = metaDataUtils.metadataService().getAllLogBaseInfo()

    getDfSchema(fields,tableName,productCode,appLogSecialEntities,baseInfoEntities)

  }

  def getDfSchema(list:List[String],tableName:String,productCode:String, appLogSecialEntities:Seq[(String, String, String, String, Int)],baseInfoEntities:List[LogBaseInfoEntity]): (List[String],Boolean) ={
    //先rename 在filter
    var fields = list
    //该产品下或者全日志或者表级别
    val appLogSpecialFieldEntities = appLogSecialEntities.filter(entity=>entity._1 == productCode || entity._1 == "log" || entity._1 == tableName )
    //字段重命名: Seq[(源字段名,字段目标名)]
    //1.baseinfo 白名单 重命名
    val baseInfoFieldMap = mutable.Map[String,String]()
    val baseInfoFields = baseInfoEntities.filter(item=>{
      item.isDeleted == false && item.getProductCode == productCode
    }).map(item => item.getFieldName)
    baseInfoFields.foreach(baseInfoField=>{
      fields.foreach(field=>{
        //字段只有忽略忽略大小写在baseInfo中，需要重命名
        if(!baseInfoField.equals(field) && baseInfoField.equalsIgnoreCase(field)){
          baseInfoFieldMap.put(field,field+"_r")
        }
      })
    })
    fields = fields.filter(field => !baseInfoFieldMap.keySet.contains(field))
    //        println(s"baseInfoFieldMap ${baseInfoFieldMap}")
    //2.黑名单重命名
    //2.1表级别
    val tabRenameMap= handleRename(appLogSpecialFieldEntities,fields,tableName)
    fields = fields.filter(field => !tabRenameMap.keySet.contains(field))
    //        println(s"tabRenameMap ${tabRenameMap}")
    //2.2产品线级别
    val productRenameMap= handleRename(appLogSpecialFieldEntities,fields,productCode)
    fields = fields.filter(field => !productRenameMap.keySet.contains(field))
    //        println(s"productRenameMap ${productRenameMap}")
    //2.3全局级别
    val logRenameMap= handleRename(appLogSpecialFieldEntities,fields,"log")
    fields = fields.filter(field => !productRenameMap.keySet.contains(field))
    //        println(s"logRenameMap ${logRenameMap}")
    val whiteRenameMap = baseInfoFieldMap.toMap ++ tabRenameMap ++ productRenameMap ++ logRenameMap
    //        println(s"whiteRenameMap ${whiteRenameMap}")
    //字段过滤器: Seq[源字段名]
    //字段删除黑名单
    val fieldFilterList = appLogSpecialFieldEntities.filter(conf => conf._3 == "fieldFilter").flatMap(conf => {
      val specialValue = conf._4
      val fieldPattern = if (specialValue.charAt(0) == '1') {
        Pattern.compile(conf._2, Pattern.CASE_INSENSITIVE)
      }
      else {
        Pattern.compile(conf._2)
      }
      val isReserve = specialValue.charAt(1) == '0'
      fields.filter(field => fieldPattern.matcher(field).find()).map(field => (field, isReserve))
    })
    //剔除白名单字段
    val whiteList = fieldFilterList.filter(_._2)
    val fieldBlackFilter = fieldFilterList.filter(item => {
      val field = item._1
      !whiteList.exists(p => p._1 == field)
    }).map(item => item._1)
    if(fieldBlackFilter.size> 0){
      println("字段删除 :"+fieldBlackFilter)
    }
    fields = fields.filter(field=> !fieldBlackFilter.contains(field))
    //处理fields中相同的字段
    val set = fields.toStream.map(field=>field.toLowerCase).toSet
    //字段列表中含有字段模糊，需要重命名
    val renameMap = mutable.Map[String,String]()
    if(fields.size != set.size){
      val dealRenameList =  fields
      var i = 1
      dealRenameList.foreach(dealField=>{
        fields = fields.filter(field=>{
          val flag = !field.equals(dealField) && field.equalsIgnoreCase(dealField)
          if(flag && !whiteRenameMap.values.toSet.contains(field)){
            renameMap.put(field,s"${field}_${i}")
            i=i+1
            false
          }else{
            true
          }
        })
      })
    }
    //flag 中间结果json文件是否删除，如果没有重命名，怎删除为true
    var flag = true
    if(renameMap.size !=0 ){
      flag=false
      println(s"renameMap : ${renameMap.toString()}")
      val e = new RuntimeException(renameMap.toString())
//      SendMail.post(e, s"[Log2Parquet new][Json2ParquetUtil][${inputPath}] field rename", Array("app-bigdata@whaley.cn"))
    }
    //        val allRenameMap = whiteRenameMap ++ renameMap.toMap
    val allRenameMap = whiteRenameMap
    //把重命名字段做到field list中
    allRenameMap.foreach(f=>{
      val oldName = f._1
      val newName = f._2
      //newName如存在原始的list中需要特殊处理
      if(!fields.contains(newName)){
        fields = fields.::(s"`$oldName` as `$newName`")
      }else{
        fields = fields.filter(field=>field !=newName)
        fields = fields.::(s"(case when `$newName` is null or `$newName` == '' then `$oldName` else `$newName` end ) as `$newName`")
      }
    })

    println(s"sql ... "+fields)
    (fields,flag)
  }


  /**
    * 处理重命名
    * @param appLogSpecialFieldEntities
    * @param fields
    * @param logPathReg
    * @return renameMap 处理重命名，leftFields剩下的字段
    */
  def handleRename(appLogSpecialFieldEntities:Seq[(String, String, String, String, Int)],fields:List[String],logPathReg:String): Map[String,String] ={
    val renameMap = appLogSpecialFieldEntities.filter(conf => conf._3 == "rename" && conf._1 == logPathReg).flatMap(conf => {
      fields.filter(field => conf._2.r.findFirstMatchIn(field).isDefined).map(field => (field, conf._4))
    }).toMap
    renameMap
  }







}
