import com.alibaba.fastjson.JSONObject
import org.junit.Test

/**
  * Created by guohao on 2017/11/22.
  */
class DataSwitch {


  @Test
  def typeSwitch(): Unit ={

    val json = new JSONObject()
    json.put("long","11")

    json.put("double2",0.01)
    json.put("string",23.4.toString)
    println(json.toString)
    json.put("double2",json.getString("double2"))





//    println(json.getString("aa").toString)
println(json.toString)
//    println(isLongValid("1.32.3"))

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
