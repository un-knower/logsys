package cn.whaley.bi.logsys.log2parquet.utils


import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.Map


/**
  * 创建人：郭浩
  * 创建时间：2017/8/4
  * 程序作用：自定义累加器，统计字段重命、字段删除的次数
  * String,Map[String,Integer]->第一个string为输入，第二个map为输出
  *
  */
class MyAccumulator extends AccumulatorV2[String,Map[String,Integer]]{

  val map = new mutable.HashMap[String,Integer]()
  /**
    * 判断是否为初始值
    * @return
    */
  override def isZero: Boolean = {
    map.isEmpty
  }

  /**
    * 拷贝累加器
    * @return
    */
  override def copy(): AccumulatorV2[String, mutable.Map[String, Integer]] = {
    val newMyAccumulator = new MyAccumulator
    val newMap = newMyAccumulator.value
    map.foreach(x=>{
      val key = x._1
      val value = x._2
      newMap += key-> value
    })
    newMyAccumulator
  }

  /**
    * 累加器重置
    */
  override def reset(): Unit = {
    map.clear()
  }


  /**
    * 累加器中添加值
    * @param v
    */
  override def add(v: String): Unit = {
    val value = map.getOrElseUpdate(v,0)+1
    map.put(v,value)
  }

  /**
    * 各个task的累加器进行合并的方法
    * @param other
    */
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Integer]]): Unit =  other match {
    case o: MyAccumulator =>{
     for(key<-o.value.keys){
        if(map.contains(key)){
          val value = o.value.getOrElseUpdate(key,0)+map.getOrElseUpdate(key,0)
          map.put(key,value)
        }else{
          map.put(key,o.value.getOrElseUpdate(key,0))
        }
      }
    }
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  /**
    * AccumulatorV2对外访问的数据结果
    * @return
    */
  override def value: mutable.Map[String, Integer] = {
    map
  }
}
