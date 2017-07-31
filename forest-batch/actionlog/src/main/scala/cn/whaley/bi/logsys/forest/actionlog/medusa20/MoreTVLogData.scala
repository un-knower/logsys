package cn.whaley.bi.logsys.forest.actionlog.medusa20

import com.alibaba.fastjson.JSONObject

/**
 * Created by Will on 2015/9/19.
 */
class MoreTVLogData(val date: String,
                    val datetime: String,
                    val ip: String,
                    val logType: String,
                    val logVersion: String,
                    val event: String,
                    val apkSeries: String,
                    val apkVersion: String,
                    val userId: String,
                    val accountId: Int,
                    val groupId: Int,
                    val bufferTimes: Int,
                    val duration: Int,
                    val liveType: String,
                    val channelSid: String,
                    val path: String,
                    val subjectCode: String,
                    val source: String,
                    val contentType: String,
                    val videoSid: String,
                    val episodeSid: String,
                    val collectType: String,
                    val collectContent: String,
                    val action: String,
                    val promotionChannel: String,
                    val weatherCode: String,
                    val productModel: String,
                    val uploadTime: String,
                    val page: String,
                    val appSid: String,
                    val accessSource: String,
                    val accessArea: String,
                    val accessLocation: String,
                    val star: String,
                    val retrievalSort: String,
                    val retrievalArea: String,
                    val retrievalYear: String,
                    val retrievalTag: String,
                    val wallpaper: String,
                    val homeType: String,
                    val homeContent: String,
                    val homeLocation: String,
                    val wechatId: String = "",
                    val title: String = "",
                    val linkType: String = "",
                    val programSid: String = "",
                    val programType: String = "",
                    val rawLog: String) extends Product with Serializable {
    override def productElement(n: Int): Any = n match {
        case 0 => date
        case 1 => datetime
        case 2 => ip
        case 3 => logType
        case 4 => logVersion
        case 5 => event
        case 6 => apkSeries
        case 7 => apkVersion
        case 8 => userId
        case 9 => accountId
        case 10 => groupId
        case 11 => bufferTimes
        case 12 => duration
        case 13 => liveType
        case 14 => channelSid
        case 15 => path
        case 16 => subjectCode
        case 17 => source
        case 18 => contentType
        case 19 => videoSid
        case 20 => episodeSid
        case 21 => collectType
        case 22 => collectContent
        case 23 => action
        case 24 => promotionChannel
        case 25 => weatherCode
        case 26 => productModel
        case 27 => uploadTime
        case 28 => page
        case 29 => appSid
        case 30 => accessSource
        case 31 => accessArea
        case 32 => accessLocation
        case 33 => star
        case 34 => retrievalSort
        case 35 => retrievalArea
        case 36 => retrievalYear
        case 37 => retrievalTag
        case 38 => wallpaper
        case 39 => homeType
        case 40 => homeContent
        case 41 => homeLocation
        case 42 => wechatId
        case 43 => title
        case 44 => linkType
        case 45 => programSid
        case 46 => programType
        case 47 => rawLog
        case _ => throw new IndexOutOfBoundsException(n.toString())
    }

    override def productArity: Int = 48

    override def canEqual(that: Any): Boolean = that.isInstanceOf[MoreTVLogData]

    def toJSONObject(): JSONObject = {
        val json = new JSONObject()

        json.put("date", date)

        json.put("datetime", datetime)

        json.put("ip", ip)

        json.put("logType", logType)

        json.put("logVersion", logVersion)

        json.put("event", event)

        json.put("apkSeries", apkSeries)

        json.put("apkVersion", apkVersion)

        json.put("userId", userId)

        json.put("accountId", accountId)

        json.put("groupId", groupId)

        json.put("bufferTimes", bufferTimes)

        json.put("duration", duration)

        json.put("liveType", liveType)

        json.put("channelSid", channelSid)

        json.put("path", path)

        json.put("subjectCode", subjectCode)

        json.put("source", source)

        json.put("contentType", contentType)

        json.put("videoSid", videoSid)

        json.put("episodeSid", episodeSid)

        json.put("collectType", collectType)

        json.put("collectContent", collectContent)

        json.put("action", action)

        json.put("promotionChannel", promotionChannel)

        json.put("weatherCode", weatherCode)

        json.put("productModel", productModel)

        json.put("uploadTime", uploadTime)

        json.put("page", page)

        json.put("appSid", appSid)

        json.put("accessSource", accessSource)

        json.put("accessArea", accessArea)

        json.put("accessLocation", accessLocation)

        json.put("star", star)

        json.put("retrievalSort", retrievalSort)

        json.put("retrievalArea", retrievalArea)

        json.put("retrievalYear", retrievalYear)

        json.put("retrievalTag", retrievalTag)

        json.put("wallpaper", wallpaper)

        json.put("homeType", homeType)

        json.put("homeContent", homeContent)

        json.put("homeLocation", homeLocation)

        json.put("wechatId", wechatId)

        json.put("title", title)

        json.put("linkType", linkType)

        json.put("programSid", programSid)

        json.put("programType", programType)

        json.put("rawLog", rawLog)

        json
    }

}
