package cn.whaley.bi.logsys.log2parquet.processor

/**
  * Created by michael on 2017/6/14.
  * 数据处理器的抽象
  */
trait Processor {

  def init():Unit
  def process():Unit
  def destroy():Unit

}
