package cn.whaley.bi.fresh

import java.io.PrintWriter

/**
  * Created by guohao on 2018/2/26.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val out = new PrintWriter("f://testScalaWrite.txt")
    for(i <- 1 to 10){
      out.append(s"$i \n")
    }
    out.flush()
    out.close()
  }

}
