package cn.whaley.bi.logsys.log2parquet

import java.net.URLDecoder

import com.alibaba.fastjson.{JSONArray, JSONObject}

import scala.collection.JavaConversions._


/**
  * Created by michael on 2017/6/22.
 */
class URLParser {


}


object URLParser {

    /**
     *
     * @param location 第一个？之前的部分
     * @param queryString 第一个？之后的部分
     */
    case class HttpURL(location: String, queryString: String)

    /**
     * 将Http地址解析成HttpURL对象
 *
     * @param url
     * @param needDecode 是否需要进行decode
     * @return
     */
    def parseHttpURL(url: String, needDecode: Boolean = true): HttpURL = {
        val decodedUrl = try {
            URLDecoder.decode(url, "utf-8")
        } catch {
            case ex: Throwable => {
                url
            }
        }
        val indexQuery = decodedUrl.indexOf('?')

        if (indexQuery >= 0) {
            HttpURL(decodedUrl.substring(0, indexQuery), decodedUrl.substring(indexQuery + 1))
        } else {
            HttpURL(url, "")
        }
    }

    /**
     * 解析http查询字符串为一个JSON对象
 *
     * @param queryString
     * @return 如果为空或长度为0，则返回一个空的JSONObject对象实例
     */
    def parseHttpQueryString(queryString: String): JSONObject = {
        val queryObj = new JSONObject()
        if (queryString != null && queryString.length() > 0) {
            var ampersandIndex, lastAmpersandIndex = 0;
            var param: String = ""
            var value: String = ""
            do {
                ampersandIndex = queryString.indexOf('&', lastAmpersandIndex) + 1;
                val subStr =
                    if (ampersandIndex > 0) {
                        val str = queryString.substring(lastAmpersandIndex, ampersandIndex - 1);
                        lastAmpersandIndex = ampersandIndex;
                        str
                    } else {
                        queryString.substring(lastAmpersandIndex);
                    }
                val firstEQ = subStr.indexOf('=')
                param = if (firstEQ <= 0) {
                    subStr
                } else {
                    subStr.substring(0, firstEQ)
                }
                value = if (firstEQ <= 0 || firstEQ == subStr.length - 1) {
                    ""
                } else {
                    subStr.substring(firstEQ + 1)
                }
                val obj = queryObj.get(param)
                if (obj != null) {
                    val arr =
                        if (!obj.isInstanceOf[JSONArray]) {
                            new JSONArray(obj :: value :: Nil)
                        } else {
                            obj.asInstanceOf[JSONArray].add(value)
                        }
                    queryObj.put(param, arr)
                } else {
                    queryObj.put(param, value)
                }
            } while (ampersandIndex > 0);
        }
        return queryObj;
    }


}
