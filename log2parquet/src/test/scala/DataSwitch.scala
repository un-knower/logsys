import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.junit.Test
import scala.collection.JavaConversions._

/**
  * Created by guohao on 2017/11/22.
  */
class DataSwitch {


  @Test
  def typeSwitch(): Unit ={

    val json = new JSONObject()
    json.put("a","aa")

    val jSONArray = new JSONArray()

    val json1 = new JSONObject()
    json1.put("contentId","a")
    json1.put("linkType",1)
    json1.put("locationindex",2)
    json1.put("recommendType",3)
    jSONArray.add(json1)

    val json2 = new JSONObject()
    json2.put("contentId","aa")
    json2.put("linkType",11)
    json2.put("locationindex",22)
    json2.put("recommendType",null)
    jSONArray.add(json2)

    json.put("jsonArray",jSONArray)
    println(s"json 000... ${json}")
//    switchJsonArrayStruct("a",json)
//    switchJsonArrayBigInt("a",json)
    switchJsonArrayString("a",json)
//    switchJsonArrayStruct("jsonArray",json)
//    switchJsonArrayBigInt("jsonArray",json)
//    switchJsonArrayString("jsonArray",json)
    println(s"json 111... ${json}")



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


  /**
    * array bigInt
    * @param key
    * @param json
    */
  def switchJsonArrayBigInt(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        val newJsonArray = new JSONArray()

        for( i<- 0 to ( jSONArray.size()-1 )){
          val value = jSONArray.get(i).toString
          if(isLongValid(value)){
            newJsonArray.add(value.toLong)
          }else{
            newJsonArray.add(0)
          }
        }
        json.put(key,newJsonArray)
      }
    }catch {
      case e:Exception=>{
        json.remove(key)
      }
    }
  }



  /**
    * array struct
    * @param key
    * @param json
    */
  def switchJsonArrayStruct(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        val newJsonArray = new JSONArray()
        for( i<- 0 to ( jSONArray.size()-1 )){
          val jsonObject = jSONArray.getJSONObject(i)

          val keys = jsonObject.keySet().toList
          keys.foreach(key=>{
            jsonObject.put(key,jsonObject.getString(key))
          })
          newJsonArray.add(jsonObject)
        }
        json.put(key,newJsonArray)
      }
    }catch {
      case e:Exception=>{
        json.remove(key)
      }
    }
  }

  /**
    * array String
    * @param key
    * @param json
    */
  def switchJsonArrayString(key:String,json:JSONObject): Unit ={
    try {
      val value = json.getString(key).trim
      val jSONArray = JSON.parseArray(value)
      if(jSONArray.size() == 0){
        json.remove(key)
      }else{
        val newJsonArray = new JSONArray()
        for( i<- 0 to ( jSONArray.size()-1 )){
          val value = jSONArray.get(i).toString
          newJsonArray.add(value)
        }
        json.put(key,newJsonArray)
      }
    }catch {
      case e:Exception=>{
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
      if(isDoubleValid(value)){
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

  def isDoubleValid(s:String)={
    //    val regex = "^([0-9]+)$"
    //转整型
    val regex = "^([0-9]+)|([0-9]+.[0-9]+)$"
    s.matches(regex)
  }

  def isLongValid(s:String)={
//    val regex = "^([0-9]+)$"
    //转整型
    val regex = "^([0-9]+)$"
    s.matches(regex)
  }


}
