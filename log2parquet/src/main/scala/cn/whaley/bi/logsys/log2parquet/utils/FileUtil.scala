package cn.whaley.bi.logsys.log2parquet.utils


import scala.collection.mutable
import scala.io.Source

/**
  * Created by guohao on 2017/8/8.
  */
object FileUtil {
 def loadFile(): Iterator[String] ={
   var path = Thread.currentThread().getContextClassLoader.getResource("fieldFilterVarification.txt").getPath
//   path = path.replace("lib/log2parquet-1.0-SNAPSHOT.jar!/fieldFilterVarification.txt","conf/fieldFilterVarification.txt").split(":")(1)


   Source.fromFile(path).getLines()
 }
  def main(args: Array[String]): Unit = {
    val lines = loadFile
    val map = new mutable.HashMap[String,String]
    lines.filter(f=> ! f.contains("#") && ! f.isEmpty).foreach(f=>{
      val key = f.split(":")(0)
      val productLine = f.split(":")(1)
      val flag = f.split(":")(2)
      if("0".equals(flag)){
        map += (key->productLine)
      }
    })
    map.foreach(f=>{
      println(f._1)
    })
  }

}
