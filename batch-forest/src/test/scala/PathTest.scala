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
    val input = "/data_warehouse/ods_origin.db/log_raw/key_day=20170828/key_hour=00/"
    val config = new Configuration()
    val fs = FileSystem.get(config)
    val fileStatus = fs.listStatus(new Path(input))




    val paths = new ArrayBuffer[String]()
    fileStatus.foreach(f=>{
      val path = f.getPath.toString
      if(path.contains("boikgpokn78sb95k") && !path.contains("boikgpokn78sb95kbqei6cc98dc5mlsr")){
        paths.append(path)
      }
    })

    println(paths(0).isInstanceOf[String])
  }

  @Test
  def test2(): Unit ={
    val input = "/data_warehouse/ods_origin.db/log_raw/key_day=20170828/key_hour=00/"

    println(input.substring(input.indexOf("/key_day")))

  }
}
