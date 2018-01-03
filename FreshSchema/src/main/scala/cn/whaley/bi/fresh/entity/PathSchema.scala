package cn.whaley.bi.fresh.entity

import scala.collection.mutable

/**
  * 创建人：郭浩
  * 创建时间：2017/12/28
  * 程序作用：
  * 数据输入：
  * 数据输出：
  */
class PathSchema {
  var path  = "" //location
  var col = new mutable.HashMap[String,String]() //表的字段和字段类型
}
