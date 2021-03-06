import scala.io.Source

/**
  * Created by guohao on 2017/12/15.
  * 生成字段类型 配置
  */
object FieldTypeChange {
  def main(args: Array[String]): Unit = {
//    switch(0,"int","Long","2","field")
//    switch(18,"int2","Long","2","table")
//    switch(356,"arrayString","ArrayString","5","table")
//    switch(368,"arrayStruct","ArrayStruct","7","table")


    b()
  }



  /**
    *
    * @param index 开始的id
    * @param fileName 文件名称
    * @param fieldType 1;String. 2:Long ,3 double 4. Array 5.ArrayString 6.ArrayLong 7.ArrayStruct
    * @param typeFlag 1,2,3,4
    * @param ruleLevel field,realLogType,table
    */
  def switch(index:Int,fileName:String,fieldType:String,typeFlag:String,ruleLevel:String): Unit = {
    val path = Thread.currentThread().getContextClassLoader.getResource(fileName).getPath
    val lines = Source.fromFile(path).getLines()
    var i = index;
    while (lines.hasNext) {
      val line = lines.next()
      val name = line.split("\\|")(0)
      val field = line.split("\\|")(1)
      i = i + 1
      val sql = if (i.toString.length == 1) {
        s"upsert into METADATA.LOG_FIELD_TYPE_INFO(id,name,fieldName,fieldType,typeFlag,ruleLevel,isDeleted,createTime,updateTime) values('00${i.toString}','${name}','${field.toLowerCase}','${fieldType}','${typeFlag}','${ruleLevel}',false,now(),now());"
      } else if(i.toString.length == 2){
        s"upsert into METADATA.LOG_FIELD_TYPE_INFO(id,name,fieldName,fieldType,typeFlag,ruleLevel,isDeleted,createTime,updateTime) values('0${i.toString}','${name}','${field.toLowerCase}','${fieldType}','${typeFlag}','${ruleLevel}',false,now(),now());"

      }else {
        s"upsert into METADATA.LOG_FIELD_TYPE_INFO(id,name,fieldName,fieldType,typeFlag,ruleLevel,isDeleted,createTime,updateTime) values('${i.toString}','${name}','${field.toLowerCase}','${fieldType}','${typeFlag}','${ruleLevel}',false,now(),now());"
      }

      println(sql)
    }
  }


  def a(): Unit ={
    val path = Thread.currentThread().getContextClassLoader.getResource("aaa").getPath
    val lines = Source.fromFile(path).getLines()
    val sb = new StringBuilder
    while (lines.hasNext) {
      val line = lines.next()
      val tableName = line.split("\\|")(0)
      val fieldName = line.split("\\|")(1)

      println("ALTER TABLE `ods_view`.`"+tableName+"` CHANGE COLUMN `"+fieldName+"` `"+fieldName+"` bigint ;")

    }

  }

  def b(): Unit ={
    val path = Thread.currentThread().getContextClassLoader.getResource("aaa").getPath
    val lines = Source.fromFile(path).getLines()
    while (lines.hasNext) {
      val line = lines.next()
      //upsert into metadata.applog_key_field_desc(appId,fieldName,fieldFlag,fieldOrder,fieldDefault,isDeleted,createTime,updateTime) values('ALL','actionId',2,1,'ALL',false,now(),now());
      val appId = line.split("\\|")(0)
      val fieldName = line.split("\\|")(1)
      val fieldFlag = line.split("\\|")(2)
      val fieldOrder = line.split("\\|")(3)
      val fieldDefault = line.split("\\|")(4)
      val isDeleted = line.split("\\|")(5)
      println(s"upsert into metadata.applog_key_field_desc(appId,fieldName,fieldFlag,fieldOrder,fieldDefault,isDeleted,createTime,updateTime) values('${appId}','${fieldName}',${fieldFlag},${fieldOrder},'${fieldDefault}',${isDeleted},now(),now());")
    }
  }


  def c(): Unit ={
    val path = Thread.currentThread().getContextClassLoader.getResource("aaa").getPath
    val lines = Source.fromFile(path).getLines()
    while (lines.hasNext) {
      val line = lines.next()
      if(line.startsWith(" line")){
        println(line)
      }
    }
  }
}
