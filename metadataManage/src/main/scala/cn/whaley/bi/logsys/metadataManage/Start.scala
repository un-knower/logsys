package cn.whaley.bi.logsys.metadataManage


import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.bi.logsys.metadataManage.util.{ConfigurationManager, ParamsParseUtil, PhoenixUtil}
import cn.whaley.bi.logsys.metadataManage.common.ParamKey._
import cn.whaley.bi.logsys.metadataManage.entity.AppLogKeyFieldDescEntity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import scala.util.matching.Regex


/**
  * Created by guohao on 2017/11/6.
  *
  * loginlog args
  * --path "/log/moretvloginlog/parquet/20171107/loginlog" --db_name ods_view --tab_prefix log --product_code "medusa" --app_code "main3x" --realLogType loginlog --key_day 20171106 --key_hour 00
  *
  * medusaAndMoretvMerger args
  *--path "/log/medusaAndMoretvMerger/20171110/[realLogType]" --db_name ods_view --tab_prefix log --product_code "medusa" --app_code "merge" --realLogType null --key_day 20171109 --key_hour 00
  *
  * dbsnapshot
  *  --path "/data_warehouse/ods_view.db/db_snapshot_[realLogType]/key_day=20171106" --db_name ods_view --tab_prefix db --product_code snapshot --app_code mysql --realLogType null --key_day 20171106 --key_hour 00
  *
  * dbsnapshot merger
  * --path "/log/dbsnapshot/parquet/20171112/[realLogType]" --db_name ods_view --tab_prefix db --product_code snapshot --app_code mysql --realLogType null --key_day 20171112 --key_hour 00
  *
  * medusa 白猫
  * white_medusa_315_update_user
  * --path "/log/medusa/parquet/20171110/white_medusa_315_update_user" --db_name ods_view --tab_prefix log --product_code "medusa" --app_code "main3x" --realLogType white_medusa_315_update_user --key_day 20171109 --key_hour 00
  * white_medusa_316_update_user
  * --path "/log/medusa/parquet/20171110/white_medusa_316_update_user" --db_name ods_view --tab_prefix log --product_code "medusa" --app_code "main3x" --realLogType white_medusa_316_update_user --key_day 20171109 --key_hour 00
  * white_medusa_update_user_by_uid
  * --path "/log/medusa/parquet/20171110/white_medusa_update_user_by_uid" --db_name ods_view --tab_prefix log --product_code "medusa" --app_code "main3x" --realLogType white_medusa_update_user_by_uid --key_day 20171109 --key_hour 00
  * white_medusa_update_user
  * --path "/log/medusa/parquet/20171110/white_medusa_update_user" --db_name ods_view --tab_prefix log --product_code "medusa" --app_code "main3x" --realLogType white_medusa_update_user --key_day 20171109 --key_hour 00
  *
  *
  *
  * whaley :buffer_middle_info
  *  --path "/log/whaley/parquet/20171112/buffer_middle_info" --db_name ods_view --tab_prefix log --product_code whaleytv --app_code main --realLogType buffer_middle_info --key_day 20171112 --key_hour 00
  * whaley:voiceusereal
  * --path "/log/whaley/parquet/20171112/voiceusereal" --db_name ods_view --tab_prefix log --product_code whaleytv --app_code main --realLogType voiceusereal --key_day 20171112 --key_hour 00
  *
  * whaley:helios-smartbrick-playermenu
  * --path "/log/whaley/parquet/20171111/helios-smartbrick-playermenu" --db_name ods_view --tab_prefix log --product_code whaleytv --app_code main --realLogType helios-smartbrick-playermenu --key_day 20171111 --key_hour 00
  *
  * whaley:helios_smartbrick_interact
  * --path "/log/whaley/parquet/20171111/helios-smartbrick-interact" --db_name ods_view --tab_prefix log --product_code whaleytv --app_code main --realLogType helios-smartbrick-interact --key_day 20171111 --key_hour 00
  *
  * whaley:helios-singer-activity
  * --path "/log/whaley/parquet/20171115/helios-singer-activity" --db_name ods_view --tab_prefix log --product_code whaleytv --app_code main --realLogType helios-singer-activity --key_day 20171115 --key_hour 00
  *
  * whaley:bulletscreen
  * --path "/log/whaley/parquet/20171115/bulletscreen" --db_name ods_view --tab_prefix log --product_code whaleytv --app_code main --realLogType bulletscreen --key_day 20171115 --key_hour 00
  *
  * medusa:medusa-tv-inputmethodusage
  * --path "/log/medusa/parquet/20171115/medusa-tv-inputmethodusage" --db_name ods_view --tab_prefix log --product_code medusa --app_code main3x --realLogType medusa-tv-inputmethodusage --key_day 20171114 --key_hour 00
  *
  *medusa:medusa-retrieval-tabview
  * --path "/log/medusa/parquet/20171030/medusa-retrieval-tabview" --db_name ods_view --tab_prefix log --product_code medusa --app_code main3x --realLogType medusa-retrieval-tabview --key_day 20171029 --key_hour 00
  *
  * medusa:medusa-home-switchinputmethod
  * --path "/log/medusa/parquet/20171109/medusa-home-switchinputmethod" --db_name ods_view --tab_prefix log --product_code medusa --app_code main3x --realLogType medusa-home-switchinputmethod --key_day 20171108 --key_hour 00
  *
  *
  */
object Start {
  val LOG=LoggerFactory.getLogger(this.getClass)
  //缺失的参数，外部传进来，值为空
  val defectParam = mutable.Map[Integer,String]()
  //默认有值得参数，外部传进来，值非空
  val defalutParam = mutable.Map[String, String]()
  var pathReg:Regex = null
  //记录缺失字段在路径正则中的角标位，以利于通过正则获取其值
  var i= 0
  def main(args: Array[String]): Unit = {
    //获取路径表达式PathExp和路径正则PathReg
    val (exp,offset,deleteOld) = getPathExpAndPathReg(args)
    val pathExp = exp._1
    pathReg =  new Regex(exp._2)
    //通过配置获取phoenix服务url和链接返回超时时间
    val metadataService = ConfigurationManager.getProperty("metadataService")
    val readTimeout = ConfigurationManager.getInteger("metadata.readTimeout")
    println(s"pathExp ...$pathExp")
    println(s"pathReg ...$pathReg")
    //根据路径规则pathExp 获取到所有的路径
    val phoenixUtil = new PhoenixUtil(metadataService,readTimeout)
    val appIdInfoMap = getMapInfo(phoenixUtil)
    val arrayBuffer = new ArrayBuffer[(String,scala.collection.mutable.Map[String,String])]()
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val fileStatus = fs.globStatus(new Path(pathExp))
    if(fileStatus == null){
      println("no match path ......")
      LOG.error("no match path ......")
      System.exit(-1)
    }
    //获取黑名单表
    val blackTableList = new ArrayBuffer[String]()
    val blackTableIter = phoenixUtil.getAllBlackTableDesc()
    while (blackTableIter.hasNext){
      val blackTableEntity = blackTableIter.next()
      blackTableList.+=(blackTableEntity.getTableName)
    }

    fileStatus.foreach(fs=>{
      val path = fs.getPath.toString
      //解析缺失参数,补全parquet 参数

      //1.通过路径path和路径正则pathReg解析出缺失参数
      val addParam = getDefectParam(path)(offset)
      //2.补全parquet 参数
      val allParam = defalutParam ++ addParam
      //校验parquet需要的参数allParam
      Breaks.breakable {
        if(!isValidParam(allParam)){
          println("path -> "+path +" param check exception ....")
          LOG.error("path -> "+path +" param check exception ....")
          Breaks.break()
        }
        //根据productCode和appCode获取appId
        val productCode = allParam.getOrElseUpdate(PRODUCT_CODE,"")
        val appCode = allParam.getOrElseUpdate(APP_CODE,"")
        val appId = getAppId(appCode,productCode,appIdInfoMap)
        if(appId.isEmpty){
          println("path -> "+path +" appId is '' ....")
          Breaks.break()
        }

        allParam.put(APPID,appId)
        val taPrefix = allParam.getOrElseUpdate("tab_prefix","")
        val realLogType = allParam.getOrElseUpdate("realLogType","")
        val tableName = s"${taPrefix}_${productCode}_${appCode}_${realLogType}"
        //过滤黑名单表
        if(!blackTableList.contains(tableName)){
          arrayBuffer.+=((path,allParam))
        }
      }

    })

    if(arrayBuffer.toArray.size == 0){
      println("no match path ...... ")
      LOG.error("no match path ...... ")
      System.exit(-1)
    }
    arrayBuffer.toStream.foreach(f=>{
      println(f._1)
      println(f._2)
    })
    println(s"arrayBuffer size is ${arrayBuffer.size}")
    //生成parquet的schema
    val msgManager = new MsgManager
    msgManager.generateMetaDataToTable(phoenixUtil,arrayBuffer.toArray,deleteOld)
  }

  def isValidParam(allParam:mutable.Map[String,String]): Boolean ={
    if(allParam.size != 7){
      return  false
    }
    val dbname = allParam.getOrElseUpdate(DB_NAME,"")
    val tab_prefix = allParam.getOrElseUpdate(TAB_PREFIX,"")
    val productCode = allParam.getOrElseUpdate(PRODUCT_CODE,"")
    val appCode = allParam.getOrElseUpdate(APP_CODE,"")
    val realLogType = allParam.getOrElseUpdate(REALLOGTYPE,"")
    val key_day = allParam.getOrElseUpdate(KEY_DAY,"")
    val key_hour = allParam.getOrElseUpdate(KEY_HOUR,"")

    if(dbname.isEmpty || tab_prefix.isEmpty ||productCode.isEmpty ||appCode.isEmpty ||realLogType.isEmpty ||key_day.isEmpty || key_hour.isEmpty){
      return false
    }
    return  true
  }

  def getDefectParam(path:String)(implicit offset:String="0"): mutable.Map[String, String] ={
    val addParam = mutable.Map[String, String]()
    pathReg findFirstMatchIn path match {
      case Some(p)=>{
        for(i<- 1 to defectParam.size){
          val value = p.group(i)
          val key = defectParam.getOrElseUpdate(i,"")
          //realLogType 中划线转换为下划线
          key match {
            case "realLogType" => addParam.put(key,value.replace("-","_"))
            case "key_day" => addParam.put(key,dateProcess(value,offset,"-"))
            case _ => addParam.put(key,value)
          }
        }
      }
      case None => println("regex match error ")
    }
    addParam
  }

  /**
    * 获取路径表达式PathExp和路径正则PathReg
    * @param args
    * @return
    */
  def getPathExpAndPathReg(args:Seq[String]):((String,String),String,String) = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val path = p.path
        val dbname = p.db_name
        val tab_prefix = p.tab_prefix
        val productCode = p.product_code
        val appCode = p.app_code
        val realLogType = p.realLogType
        val key_day = p.key_day
        val key_hour = p.key_hour
        val offset = p.offset
        val deleteOld = p.deleteOld

        //解析路径规则->路径表达式pathExp、路径正则pathReg、缺失的参数、部分parquet参数
        var regExpPath = analysisPath(path,(path,path),"(\\w+)",DB_NAME,dbname)
        regExpPath = analysisPath(path,regExpPath,"([a-zA-Z-0-9]+)",TAB_PREFIX,tab_prefix)
        regExpPath = analysisPath(path,regExpPath,"([a-zA-Z-0-9]+)",PRODUCT_CODE,productCode)
        regExpPath = analysisPath(path,regExpPath,"(global_menu_2|[a-zA-Z-0-9]+)",APP_CODE,appCode)
        regExpPath = analysisPath(path,regExpPath,"([\\w-]+)",REALLOGTYPE,realLogType)
        regExpPath = analysisPath(path,regExpPath,"([0-9]+)",KEY_DAY,key_day)(offset)
        regExpPath = analysisPath(path,regExpPath,"([0-9]+)",KEY_HOUR,key_hour)
        (regExpPath,offset,deleteOld)
      }
      case None => {
        throw new RuntimeException("parameters error ... ")
      }
    }
  }


  /**
    *
    * @param path 用于获取缺失字段的角标位，来确认正则group(n)
    * @param pathExpAndPathReg 路径的表达式和路径的正则
    * @param regExp  key 的正则表达式
    * @param key
    * @param value
    * @param offset
    * @return
    */
  def analysisPath(path:String,pathExpAndPathReg:(String,String),regExp:String,key:String,value:String)
                  (implicit offset:String="0"): (String,String) ={
    var pathExp:String = pathExpAndPathReg._1
    var pathReg:String = pathExpAndPathReg._2
    val oldValue = s"[$key]"
    if(value.isEmpty || value == "null" ){
      pathExp = pathExp.replace(oldValue,"*")
      pathReg = pathReg.replace(oldValue,regExp)
      val str = path.split(key)(0)
//      i=i+1
      defectParam.put(containsNum(str),key)
    }else{
      defalutParam.put(key,value)
      //特殊处理key_day 与 location 时间不一致问题
      if("key_day".equals(key)){
        pathExp = pathExp.replace(oldValue,dateProcess(value,offset,"+"))
        pathReg = pathReg.replace(oldValue,dateProcess(value,offset,"+"))
      }else{
        pathExp = pathExp.replace(oldValue,value)
        pathReg = pathReg.replace(oldValue,value)
      }
    }
    (pathExp,pathReg)
  }

  /**
    * 处理缺失字段的角标位
    * @param str
    * @return
    */
  def containsNum(str:String): Integer ={
    var s = str
    var num:Integer = 0
    while (s.contains("[")){
      num = num + 1
      s = s.substring(0,s.lastIndexOf("[")-1)
    }
    num
  }

  /**
    * 处理key_day  的 offset时间
    * @param date
    * @param offset
    * @param tag 从key_day->path offset为正， path->key_day offset 为负值
    * @return
    */
  def dateProcess(date:String ,offset:String,tag:String): String ={
    val  df:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal:Calendar=Calendar.getInstance()
    cal.setTime(df.parse(date))
    tag match {
      case "+" =>cal.add(Calendar.DATE,Integer.valueOf(offset))
      case "-" => cal.add(Calendar.DATE,-Integer.valueOf(offset))
      case _  =>
    }
    df.format(cal.getTime)
  }

  /**
    * map
    * key :appcode_productcode
    * value: appId
    * @return
    */
  def getMapInfo(phoenixUtil:PhoenixUtil): mutable.HashMap[String,String] ={
    val itemsIterator = phoenixUtil.getAllAppLogKeyFieldDesc()
    val array = ArrayBuffer[AppLogKeyFieldDescEntity]()
    while(itemsIterator.hasNext){
      val item = itemsIterator.next()
      array.+=(item)
    }
    val items = array.filter(item=>{
      item.getFieldFlag == 1 && item.isDeleted == false && (item.getFieldName == "app_code" || item.getFieldName == "product_code")
    })
      .map(item=>(item.getAppId, item.getFieldName, item.getFieldDefault))
    val appcode_productcode_appId = mutable.HashMap[String,String]()
    items.map(row => row._1).distinct.map(appId => {
      val appcodes = items.filter(row=> row._1 == appId  && row._2 =="app_code" &&  row._3 != null ).map(row=>row._3).distinct
      val productcodes = items.filter(row=> row._1 == appId  && row._2 =="product_code" ).map(row=>row._3).distinct
      appcodes.foreach(appcode=>{
        productcodes.foreach(productcode=>{
          val key = s"${appcode}_${productcode}"
          val value = appId
          appcode_productcode_appId +=(key->value)
        })
      })
    })
    appcode_productcode_appId
  }

  /**
    * 获取appId
    * @param appCode
    * @param productCode
    * @return
    */
  def getAppId(appCode:String,productCode:String,map:mutable.HashMap[String,String]):String={
    val key = s"${appCode}_${productCode}"
    val appId = map.getOrElseUpdate(key,"")
    appId
  }
}
