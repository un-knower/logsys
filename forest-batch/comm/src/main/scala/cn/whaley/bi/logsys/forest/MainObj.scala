package cn.whaley.bi.logsys.forest

import cn.whaley.bi.logsys.forest.Traits.ExecutedTrait
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import sun.misc.{SignalHandler, Signal}

/**
 * Created by fj on 16/10/30.
 *
 * forest程序主入口
 */
object MainObj {

    val LOG=LoggerFactory.getLogger(this.getClass)

    /**
     * 程序入口
     * @param args
     * 第一个参数为运行的类名,之后的所有参数为该类所需要的入参列表,如:
     * MsgProcExecutor -s 20160601
     * 类名以.开头，代表与cn.whaley.bi.logsys.forest的相对路径
     * 类名不以.开头，且包含.则代表全路径名
     * 类名不以.开头，且不包含.则代表cn.whaley.bi.logsys.forest下的类
     */
    def main(args: Array[String]) {
        require(args.length >= 1)

        val cls = args(0)
        val index = cls.indexOf('.')
        val clsQualityName = if (index < 0) {
            s"${this.getClass.getPackage.getName}.${cls}"
        } else if (index == 0) {
            s"${this.getClass.getPackage.getName}${cls}"
        } else {
            cls
        }

        val clz = Class.forName(clsQualityName)
        val executedTrait = clz.newInstance().asInstanceOf[ExecutedTrait]

        Runtime.getRuntime().addShutdownHook(
            new Thread() {
                override def run(): Unit = {
                    try {
                        LOG.info(s"## stopping the executor ${clsQualityName}");
                        executedTrait.shutdown(true);
                        println(s"## stopped the executor ${clsQualityName}");
                    } catch {
                        case ex: Throwable => {
                            LOG.error("",ex);
                            LOG.info(s"## something goes wrong when stopping executor ${clsQualityName}");
                        }
                    } finally {
                        LOG.info(s"## executor ${clsQualityName} is down.");
                    }
                }
            }
        );

        val execArgs = args.toList.tail.toArray
        executedTrait.execute(execArgs)
    }

    /*
     //TERM（kill -15）、USR1（kill -10）、USR2（kill -12）
        //kill -l 查看linux下的kill信号
        val handler = new SignalHandler {
            override def handle(signal: Signal): Unit = {
                println(s"${signal.getName} is received. shutdown...");
                executedTrait.shutdown(true);
            }
        }
        registerSignalHandler(new Signal("TERM"), handler);
        registerSignalHandler(new Signal("USR1"), handler);
        registerSignalHandler(new Signal("USR2"), handler);
     */
    def registerSignalHandler(signal: Signal, handler: SignalHandler): Unit = {
        try {
            Signal.handle(signal, handler);
            println(s"register signal handler [${signal.getName}].")
        } catch {
            case ex: Throwable => {
                println(s"cann't register signal handler [${signal.getName}]. ${ex.getMessage}")
            }
        }
    }

}
