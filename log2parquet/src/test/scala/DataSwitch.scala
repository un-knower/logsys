import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.junit.Test

/**
  * Created by guohao on 2017/11/22.
  */
class DataSwitch {


  @Test
  def typeSwitch(): Unit ={

    val json = new JSONObject()
    json.put("a",11)
    val jSONArray = new JSONArray()

    val json1 = new JSONObject()
    json1.put("1",11)
    jSONArray.add(json1)
    val json2 = new JSONObject()
    json2.put("2",22)
    jSONArray.add(json2)
    json.put("array",jSONArray)
    println(s"1... ${json}")
    json.keySet().toArray(new Array[String](0)).foreach(key=>{
      val value = json.getString(key)
      json.put(key,value)
    })


    println(s"2... ${json}")


    switchJsonArray("a",json)

    println(s"3... ${json}")


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
