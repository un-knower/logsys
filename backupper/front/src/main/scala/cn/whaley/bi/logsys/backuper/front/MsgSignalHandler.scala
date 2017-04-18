package cn.whaley.bi.logsys.backuper.front

import cn.whaley.bi.logsys.backuper.front.global.Constants
import sun.misc.{Signal, SignalHandler}

/**
  * Created by Will on 11/30/2016.
  * 处理信号，停止程序
  */
class MsgSignalHandler extends SignalHandler{

  override def handle(signal: Signal): Unit = {
    if(signal.getNumber == 15){
      Constants.SIGNAL_SHOULD_STOP = true
      println(Constants.SIGNAL_SHOULD_STOP)
    }else{
      println(s"Can not recognize signal: ${signal.getName} [${signal.getNumber}]")
      println(Constants.SIGNAL_SHOULD_STOP)
    }
  }



}
