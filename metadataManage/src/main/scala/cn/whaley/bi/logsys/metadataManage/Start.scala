package cn.whaley.bi.logsys.metadataManage

import cn.whaley.bi.logsys.metadataManage.util.{ConfigurationManager, ParamsParseUtil, PhoenixUtil}
import cn.whaley.bi.logsys.metadataManage.common.ParamKey._
import cn.whaley.bi.logsys.metadataManage.entity.AppLogKeyFieldDescEntity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex


/**
  * Created by guohao on 2017/11/6.
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
    val exp = getPathExpAndPathReg(args)
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

    fileStatus.foreach(fs=>{
      val path = fs.getPath.toString
      //解析缺失参数,补全parquet 参数

      //1.通过路径path和路径正则pathReg解析出缺失参数
      val addParam = getDefectParam(path)
      //2.补全parquet 参数
      val allParam = defalutParam ++ addParam
      //校验parquet需要的参数allParam
      if(!isValidParam(allParam)){
        LOG.error("param check exception ....")
        System.exit(-1)
      }
      //根据productCode和appCode获取appId
      val productCode = allParam.getOrElseUpdate(PRODUCT_CODE,"")
      val appCode = allParam.getOrElseUpdate(APP_CODE,"")
      val appId = getAppId(appCode,productCode,appIdInfoMap)
      allParam.put(APPID,appId)
      arrayBuffer.+=((path,allParam))
    })

    if(arrayBuffer.toArray.size == 0){
      LOG.error("no match path ... ")
      System.exit(-1)
    }
    arrayBuffer.toStream.foreach(f=>{
      println(f._1)
      println(f._2)
    })


    //生成parquet的schema
    val msgManager = new MsgManager
    msgManager.generateMetaDataToTable(phoenixUtil,arrayBuffer.toArray,"111")
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

  def getDefectParam(path:String): mutable.Map[String, String] ={
    val addParam = mutable.Map[String, String]()
    pathReg findFirstMatchIn path match {
      case Some(p)=>{
        for(i<- 1 to defectParam.size){
          val value = p.group(i)
          val key = defectParam.getOrElseUpdate(i,"")
          addParam.put(key,value)
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
  def getPathExpAndPathReg(args:Seq[String]):(String,String) = {
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

        //解析路径规则->路径表达式pathExp、路径正则pathReg、缺失的参数、部分parquet参数
        var regExpPath = analysisPath((path,path),"(\\w+)",DB_NAME,dbname)
        regExpPath = analysisPath(regExpPath,"([a-zA-Z-0-9]+)",TAB_PREFIX,tab_prefix)
        regExpPath = analysisPath(regExpPath,"([a-zA-Z-0-9]+)",PRODUCT_CODE,productCode)
        regExpPath = analysisPath(regExpPath,"(global_menu_2|[a-zA-Z-0-9]+)",APP_CODE,appCode)
        regExpPath = analysisPath(regExpPath,"(\\w+)",REALLOGTYPE,realLogType)
        regExpPath = analysisPath(regExpPath,"([0-9]+)",KEY_DAY,key_day)
        regExpPath = analysisPath(regExpPath,"([0-9]+)",KEY_HOUR,key_hour)
        regExpPath
      }
      case None => {
        throw new RuntimeException("parameters error ... ")
      }
    }
  }


  /**
    *
    * @param pathExpAndPathReg 路径的表达式和路径的正则
    * @param regExp key 的正则表达式
    * @param key
    * @param value
    * @return
    */
  def analysisPath(pathExpAndPathReg:(String,String),regExp:String,key:String,value:String): (String,String) ={
    var pathExp:String = pathExpAndPathReg._1
    var pathReg:String = pathExpAndPathReg._2
    val oldValue = s"[$key]"

    if(value.isEmpty || value == "null" ){
      pathExp = pathExp.replace(oldValue,"*")
      pathReg = pathReg.replace(oldValue,regExp)
      i=i+1
      defectParam.put(i,key)
    }else{
      defalutParam.put(key,value)
      pathExp = pathExp.replace(oldValue,value)
      pathReg = pathReg.replace(oldValue,value)
    }
    (pathExp,pathReg)
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
