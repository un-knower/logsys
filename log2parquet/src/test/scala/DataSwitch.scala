import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.junit.Test

/**
  * Created by guohao on 2017/11/22.
  */
class DataSwitch {


  @Test
  def typeSwitch(): Unit ={

    val json = new JSONObject()
    json.put("a",null)
    println(s"3... ${json}")

    val value = json.getString("a").trim
    println(s"311... ${value}")



//    println(json.getString("aa").toString)
//    println(isLongValid("1.32.3"))

  }


  def switchJsonArray(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      json.put(key,jSONArray)
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.remove(key)
      }
    }
  }


  def switchString(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      if(isLongValid(value)){
        json.put(key,value.toLong)
      }else{
        json.put(key,0)
      }
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.put(key,0)
      }
    }
  }


  def switchLong(key:String,json:JSONObject): Unit ={
    try {
        val value = json.getString(key).trim
        if(isLongValid(value)){
          json.put(key,value.toLong)
        }else{
          json.put(key,0)
        }
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.put(key,0)
      }
    }
  }

  def switchDouble(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      if(isValid(value)){
        json.put(key,value.toDouble)
      }else{
        json.put(key,0.0)
      }
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        json.put(key,0.0)
      }
    }
  }
  //转String
  //转int
  //转long
  //转double

  def isValid(s:String)={
    //    val regex = "^([0-9]+)$"
    //转整型
    val regex = "^([0-9]+)|([0-9]+.[0-9]+)$"
    s.matches(regex)
  }

  def isLongValid(s:String)={
//    val regex = "^([0-9]+)$"
    //转整型
    val regex = "^([0-9]+)|([0-9]+.[0-9]+)$"
    s.matches(regex)
  }

}
