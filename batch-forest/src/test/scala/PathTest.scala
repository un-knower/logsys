import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
  * Created by guohao on 2017/8/31.
  */
class PathTest {

  @Test
  def test(): Unit ={
    val input = "/data_warehouse/ods_origin.db/log_raw/key_day=20170920/key_hour=00/"
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val fileStatus = fs.listStatus(new Path(input))




    var paths = new ArrayBuffer[String]()
    fileStatus.foreach(f=>{
      val path = f.getPath.toString
      if(path.split("/").size == 9 && path.split("/")(8).startsWith("boikgpokn78sb95k")){
        paths.append(path)
      }
    })


    //过滤的appId,不需要处理
    val filterAppId = Array("boikgpokn78sb95k0000000000000000",
      "boikgpokn78sb95ktmsc1bnkbe9pbhgu",
      "boikgpokn78sb95ktmsc1bnkfipphckl",
      "boikgpokn78sb95kjtihcg268dc5mlsr",
      "boikgpokn78sb95kbqei6cc98dc5mlsr",
      "boikgpokn78sb95kkls3bhmtjqosocdj",
      "boikgpokn78sb95kkls3bhmtichjhhm8",
      "boikgpokn78sb95ktmsc1bnkklf477ap",
      "boikgpokn78sb95kicggqhbkepkseljn")

    val appId = "all"

    paths = appId match {
      //过滤不需要转换的appId
      case "all" =>{
        paths = paths.filter(path=>{
          if(!filterAppId.contains(path.split("/")(8).split("\\.")(0))){
            true
          }else{
            false
          }
        })
        paths
      }
      //指定的appId
      case _ =>{
        paths = paths.filter(p=>{
          p.contains(appId)
        })
        paths
      }

    }

    paths.foreach(println(_))
  }

  @Test
  def test2(): Unit ={
    val input = "/data_warehouse/ods_origin.db/log_raw/key_day=20170828/key_hour=00/"

    println(input.substring(input.indexOf("/key_day")))

  }
}
