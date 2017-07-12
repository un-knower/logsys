package cn.whaley.bi.logsys.log2parquet.moretv2x

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

    def toJSONObject: JSONObject = {
        val json = new JSONObject()

        if(date != null && date.nonEmpty) json.put("date", date)

        if(datetime != null && datetime.nonEmpty) json.put("datetime", datetime)

        if(ip != null && ip.nonEmpty) json.put("ip", ip)

        if(logType != null && logType.nonEmpty) json.put("logType", logType)

        if(logVersion != null && logVersion.nonEmpty) json.put("logVersion", logVersion)

        if(event != null && event.nonEmpty) json.put("event", event)

        if(apkSeries != null && apkSeries.nonEmpty) json.put("apkSeries", apkSeries)

        if(apkVersion != null && apkVersion.nonEmpty) json.put("apkVersion", apkVersion)

        if(userId != null && userId.nonEmpty) json.put("userId", userId)

        json.put("accountId", accountId)

        json.put("groupId", groupId)

        json.put("bufferTimes", bufferTimes)

        json.put("duration", duration)

        if(liveType != null && liveType.nonEmpty) json.put("liveType", liveType)

        if(channelSid != null && channelSid.nonEmpty) json.put("channelSid", channelSid)

        if(path != null && path.nonEmpty) json.put("path", path)

        if(subjectCode != null && subjectCode.nonEmpty) json.put("subjectCode", subjectCode)

        if(source != null && source.nonEmpty) json.put("source", source)

        if(contentType != null && contentType.nonEmpty) json.put("contentType", contentType)

        if(videoSid != null && videoSid.nonEmpty) json.put("videoSid", videoSid)

        if(episodeSid != null && episodeSid.nonEmpty) json.put("episodeSid", episodeSid)

        if(collectType != null && collectType.nonEmpty) json.put("collectType", collectType)

        if(collectContent != null && collectContent.nonEmpty) json.put("collectContent", collectContent)

        if(action != null && action.nonEmpty) json.put("action", action)

        if(promotionChannel != null && promotionChannel.nonEmpty) json.put("promotionChannel", promotionChannel)

        if(weatherCode != null && weatherCode.nonEmpty) json.put("weatherCode", weatherCode)

        if(productModel != null && productModel.nonEmpty) json.put("productModel", productModel)

        if(uploadTime != null && uploadTime.nonEmpty) json.put("uploadTime", uploadTime)

        if(page != null && page.nonEmpty) json.put("page", page)

        if(appSid != null && appSid.nonEmpty) json.put("appSid", appSid)

        if(accessSource != null && accessSource.nonEmpty) json.put("accessSource", accessSource)

        if(accessArea != null && accessArea.nonEmpty) json.put("accessArea", accessArea)

        if(accessLocation != null && accessLocation.nonEmpty) json.put("accessLocation", accessLocation)

        if(star != null && star.nonEmpty) json.put("star", star)

        if(retrievalSort != null && retrievalSort.nonEmpty) json.put("retrievalSort", retrievalSort)

        if(retrievalArea != null && retrievalArea.nonEmpty) json.put("retrievalArea", retrievalArea)

        if(retrievalYear != null && retrievalYear.nonEmpty) json.put("retrievalYear", retrievalYear)

        if(retrievalTag != null && retrievalTag.nonEmpty) json.put("retrievalTag", retrievalTag)

        if(wallpaper != null && wallpaper.nonEmpty) json.put("wallpaper", wallpaper)

        if(homeType != null && homeType.nonEmpty) json.put("homeType", homeType)

        if(homeContent != null && homeContent.nonEmpty) json.put("homeContent", homeContent)

        if(homeLocation != null && homeLocation.nonEmpty) json.put("homeLocation", homeLocation)

        if(wechatId != null && wechatId.nonEmpty) json.put("wechatId", wechatId)

        if(title != null && title.nonEmpty) json.put("title", title)

        if(linkType != null && linkType.nonEmpty) json.put("linkType", linkType)

        if(programSid != null && programSid.nonEmpty) json.put("programSid", programSid)

        if(programType != null && programType.nonEmpty) json.put("programType", programType)

        if(rawLog != null && rawLog.nonEmpty) json.put("rawLog", rawLog)

        json
    }

}
