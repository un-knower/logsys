package cn.whaley.bi.logsys.batchforest.entity

import com.alibaba.fastjson.JSONObject

/**
  * Created by guohao on 2017/8/28.
  * 归集层消息实体对象
  */
class MsgEntity(from: JSONObject) extends JSONObject(from){
  def msgId: String = {
    this.getString(MsgEntity.KEY_MSG_ID)
  }

  def msgVersion: String = {
    this.getString(MsgEntity.KEY_MSG_VERSION)
  }

  def msgSite: String = {
    this.getString(MsgEntity.KEY_MSG_SITE)
  }

  def msgSource: String = {
    this.getString(MsgEntity.KEY_MSG_SOURCE)
  }

  def msgFormat: String = {
    this.getString(MsgEntity.KEY_MSG_FORMAT)
  }

  def msgSignFlag: String = {
    this.getString(MsgEntity.KEY_MSG_SIGN_FLAG)
  }

  def msgBody: String = {
    this.getString(MsgEntity.KEY_MSG_BODY)
  }

}

object MsgEntity{
  val KEY_MSG_ID = "msgId"
  val KEY_MSG_VERSION = "msgVersion"
  val KEY_MSG_SITE = "msgSite"
  val KEY_MSG_SOURCE = "msgSource"
  val KEY_MSG_FORMAT = "msgFormat"
  val KEY_MSG_SIGN_FLAG = "msgSignFlag"
  val KEY_MSG_BODY = "msgBody"
}

