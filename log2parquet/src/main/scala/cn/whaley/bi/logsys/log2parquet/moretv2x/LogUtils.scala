package cn.whaley.bi.logsys.log2parquet.moretv2x

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * Created by Will on 2015/7/16.
 * The utility of log str process.
 */
object LogUtils {

    val regexIpA = "\"(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})".r
    val regexIpB = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})".r
    val invalidChars = List(" ", ",", ";", "{", "}", "(", ")", "\\n", "\\t", "=", "/", "\\", "..", "`",
        "!", "@", "#", "$", "%", "^", "&", "*", "'", ":", "[", "]", "?", "<", ">")
    val regexWord = "^\\w+$".r

    /**
     * split seriesVersion
     *
     * @param seriesVersion series_version
     * @return (series,version)
     */
    def splitSeriesVersion(seriesVersion: String) = {
        val idx = seriesVersion.lastIndexOf('_')
        if (idx < 0) Array("", "")
        else
            Array(seriesVersion.substring(0, idx), seriesVersion.substring(idx + 1))
    }

    /**
     * split userId,accountId,groupId
     *
     * @param idsStr userId-accountId-groupId str
     * @return (userId,accountId,groupId)
     */
    def getIds(idsStr: String) = {
        val ids = idsStr.split("-")
        if (ids.length == 3) {
            val userId = ids(0)
            val accountId = if (ids(1).length > 0) ids(1) else LogFields.ACCOUNT_ID_DEFAULT
            val groupId = if (ids(2).length > 0) ids(2) else LogFields.GROUP_ID_DEFAULT
            (userId, accountId, groupId)
        } else if (ids.length == 2) {
            val userId = ids(0)
            val accountId = if (ids(1).length > 0) ids(1) else LogFields.ACCOUNT_ID_DEFAULT
            (userId, accountId, LogFields.GROUP_ID_DEFAULT)
        } else (ids(0), LogFields.ACCOUNT_ID_DEFAULT, LogFields.GROUP_ID_DEFAULT)
    }

    /**
     * split appId,subjectCode
     *
     * @param idStr appId-subjectCode
     * @return (appId,subjectCode)
     */
    def getAppIdAndSubjectCode(idStr: String) = {
        val ids = idStr.split("-")
        if (ids.length == 2) {
            (ids(0), ids(1))
        } else (ids(0), "")
    }

    /**
     * get page & path from pageview raw log
     *
     * @param ppStr part of the raw log
     * @return (page,path)
     */
    def getPagePath(ppStr: String) = {
        val idxPage = ppStr.indexOf("%26page%3D")
        val idxPath = ppStr.indexOf("%26path%3D")
        if (idxPage < 0) ("", "")
        else if (idxPath < 0) (ppStr.substring(idxPage + 10), "")
        else (ppStr.substring(idxPage + 10, idxPath), ppStr.substring(idxPath + 10))
    }

    def keyProcess(key: String) = {
        if (key == "ProductModel") "productModel"
        else {
            regexWord findFirstIn key match {
                case Some(m) => key
                case None => "corruptKey"
            }
        }
    }

    def getLogType(msgStr:String):String = {
        if(msgStr != null && msgStr.nonEmpty){
            val idx = msgStr.indexOf('-')
            msgStr.substring(0,idx)
        }else LogType.CORRUPT
    }



}
