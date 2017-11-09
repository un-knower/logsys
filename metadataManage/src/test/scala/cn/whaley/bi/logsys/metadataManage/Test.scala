package cn.whaley.bi.logsys.metadataManage

import org.scalatest.Suite

/**
  * Created by guohao on 2017/11/6.
  */
class Test extends  Suite{

  val regExp = ("/data_warehouse/ods_view.db/log_([a-zA-Z-0-9]+)_(global_menu_2|[a-zA-Z-0-9]+)_(\\w+)/key_day=20171106/key_hour=11").r

  def test(): Unit ={
    val path = "hdfs://hans/data_warehouse/ods_view.db/log_whaleytv_wui20_quickentryreplace/key_day=20171106/key_hour=11"
    println(regExp)
    regExp findFirstMatchIn path match {
      case Some(p)=>{
        println(p.group(1))
        println(p.group(2))
        println(p.group(3))
      }
      case None =>
    }



 }

  def test2(): Unit ={
    val path = "/data_warehouse/ods_view.db/log_whaleytv_main_play_ert/key_day=20171106/key_hour=11"
    val regExp = """/data_warehouse/ods_view.db/log_([a-zA-Z-0-9]+)_(global_menu_2|[a-zA-Z-0-9]+)_(\\w+)/key_day=20171106/key_hour=11"""


  }


}
