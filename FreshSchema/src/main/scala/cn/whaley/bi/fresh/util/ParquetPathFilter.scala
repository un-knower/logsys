package cn.whaley.bi.fresh.util

import org.apache.hadoop.fs.{Path, PathFilter}

/**
  * Created by baozhiwang on 2017/7/5.
  */
class ParquetPathFilter extends PathFilter{
  val regex:String=".*parquet$"
  override  def accept(path:Path): Boolean = {
    path.toString().matches(regex);
  }
}
