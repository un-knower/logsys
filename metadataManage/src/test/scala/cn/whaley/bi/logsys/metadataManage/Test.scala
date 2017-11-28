package cn.whaley.bi.logsys.metadataManage

import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.Suite

/**
  * Created by guohao on 2017/11/6.
  */
class Test extends  Suite{

  val regExp = ("/data_warehouse/(\\w+).db/([a-zA-Z-0-9]+)_([a-zA-Z-0-9]+)_(global_menu_2|[a-zA-Z-0-9]+)_(\\w+)/key_day=20171109/key_hour=19").r

  def test(): Unit ={
    val path = "hdfs://hans/data_warehouse/ods_view.db/log_eagle_main_dialog_menu_click/key_day=20171109/key_hour=19"
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

    println(dateProcess("20171127","-1"))

  }


  def dateProcess(date:String ,offset:String): String ={
    val  df:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal:Calendar=Calendar.getInstance()
    cal.setTime(df.parse(date))
    cal.add(Calendar.DATE,Integer.valueOf(offset))
    df.format(cal.getTime)
  }



}
