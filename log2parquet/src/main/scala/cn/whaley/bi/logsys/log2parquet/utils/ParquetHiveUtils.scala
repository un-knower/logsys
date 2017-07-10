package cn.whaley.bi.logsys.log2parquet.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{PathFilter, FileSystem, FileStatus, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.{MessageType, Type}

import scala.collection.JavaConversions._

/**
  * Created by michael on 2017/7/4.
  */
object ParquetHiveUtils {

  private val typeMap = Map(
    "MAP" -> "string"
    , "LIST" -> "string"
    , "UTF8" -> "string"
    , "MAP_KEY_VALUE" -> "string"
    , "ENUM" -> "string"
    , "DECIMAL" -> "decimal"
    , "DATE" -> "date"
    , "TIME_MILLIS" -> "timestamp"
    , "TIMESTAMP_MILLIS" -> "timestamp"
    , "UINT_8" -> "int"
    , "UINT_16" -> "int"
    , "UINT_32" -> "int"
    , "UINT_64" -> "bigint"
    , "INT8" -> "tinyint"
    , "INT16" -> "smallint"
    , "INT32" -> "int"
    , "INT64" -> "bigint"
    , "UINT8" -> "int"
    , "UINT16" -> "int"
    , "UINT32" -> "int"
    , "UINT64" -> "bigint"
    , "INT8" -> "tinyint"
    , "INT16" -> "smallint"
    , "INT32" -> "int"
    , "INT64" -> "bigint"
    , "LONG" -> "bigint"
    , "JSON" -> "string"
    , "BSON" -> "string"
    , "INTERVAL" -> "string"
  )

  /**
    *
    * @param tp
    * @return (isPrimitive:Boolean,typeName:String,typeString:String)
    */
  private def getParquetTypeInfo(tp: Type): (Boolean, String, String) = {
    if (tp.isPrimitive) {
      val parquetTypeName = tp.asPrimitiveType().getPrimitiveTypeName.name()
      (true, parquetTypeName, tp.toString)
    } else {
      val ty = tp.asGroupType()
      val typeName = if (ty.getOriginalType != null) {
        ty.getOriginalType.name()
      } else {
        ty.getClass.getSimpleName
      }
      (false, typeName, tp.toString)
    }
  }

  /**
    *
    * @param tp
    * @return (isPrimitive:Boolean,hiveTypeName:String)
    */
  private def getHiveTypeInfo(tp: org.apache.parquet.schema.Type): (Boolean, String) = {
    if (tp.isPrimitive) {
      val parquetTypeName = tp.asPrimitiveType().getPrimitiveTypeName.name().toUpperCase()
      (true, typeMap.getOrElse(parquetTypeName, "string"))
    } else {
      val ty = tp.asGroupType()
      val isList = ty.getOriginalType != null && ty.getOriginalType.name() == "LIST"
      if (isList) {
        (false, "array")
      } else {
        (false, "struct")
      }
    }
  }

  private def parseSQLFiledInfo(f: Type, parent: Type): (String, String, String) = {
    val spl = if (parent != null) ":" else " "
    val fName = f.getName
    val hiveTypeInfo = getHiveTypeInfo(f)
    val isPrimitive = hiveTypeInfo._1
    val hiveTypeName = hiveTypeInfo._2
    val fieldInfo = if (isPrimitive) {
      (fName, hiveTypeName, s"`${fName}`${spl}${hiveTypeName}")
    } else {
      val ty = f.asGroupType()
      val info = if (hiveTypeName == "array") {
        //数组类型
        val elementType = ty.getFields.get(0).asGroupType().getFields.get(0)
        if (elementType.isPrimitive) {
          //数组元素为简单类型
          val element = elementType.asPrimitiveType()
          val hiveName = getHiveTypeInfo(element.asPrimitiveType())._2
          (fName, "array", s"`${fName}`${spl}array<${hiveName}>")
        } else {
          //数组元素为符合类型
          val elementInfo = parseSQLFiledInfo(elementType, f)
          //字段名加上两个`和一个空格
          val arrayTypeInfo = elementInfo._3.substring(elementInfo._1.length + 3)
          (fName, "array", s"`${fName}`${spl}array<${arrayTypeInfo}>")
        }
      } else {
        //结构体类型
        val subInfo = ty.getFields.map(sub => {
          parseSQLFiledInfo(sub, f)
        }).map(_._3).mkString(",")
        (fName, "struct", s"`${fName}`${spl}struct<${subInfo}>")
      }
      info
    }
    fieldInfo
  }

  /**
    * 从MessageType解析sql字段信息
    *
    * @param schema
    * @return Seq[(fieldName:String,fieldType:String,fieldSql:String,rowType:String,rowInfo:String)]
    */
  def parseSQLFiledInfos(schema: MessageType): Seq[(String, String, String, String, String)] = {
    schema.getFields.map(f => {
      val info = parseSQLFiledInfo(f, null)
      val rowType = getParquetTypeInfo(f)
      (info._1, info._2, info._3, rowType._2, rowType._3)
    })
  }

  /**
    * 从parquet文件中解析sql字段信息
    *
    * @param parquetFilePath
    * @return Seq[(fieldName:String,fieldType:String,fieldSql:String,rowType:String,rowInfo:String)]
    */
  def parseSQLFieldInfos(parquetFilePath: Path): Seq[(String, String, String, String, String)] = {
    val configuration = new Configuration(true);
    val readFooter: ParquetMetadata = ParquetFileReader.readFooter(configuration, parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    val schema = readFooter.getFileMetaData().getSchema();
    parseSQLFiledInfos(schema)
  }


  def getParquetFilesFromHDFS(dir: String): Array[FileStatus] = {
    val conf = new Configuration(true)
    val fs = FileSystem.get(conf)
    val input_dir = new Path(dir)
    if(fs.exists(input_dir)){
      val filter=new ParquetPathFilter
      val hdfs_files = fs.listStatus(input_dir,filter)
      hdfs_files
    }else{
      println("-------dir_not_exist:"+dir)
      val empty=new Array[FileStatus](0)
      empty
    }
  }
}



