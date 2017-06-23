package cn.whaley.bi.logsys.log2parquet

import java.util.regex.Pattern

/**
  * Created by baozhiwang on 2017/6/22.
  */
object Test extends App{
  val regex="appIdForProcessGroups"
  val p= Pattern.compile(regex)
  val m = p.matcher("appIdForProcessGroup.boikgpokn78sb95ktmsc1bnkechpgj9l")
  println(m.find())


}
