package cn.whaley.bi.logsys.log2parquet

import java.util.Properties

import cn.whaley.bi.logsys.common.ConfManager
import cn.whaley.bi.logsys.log2parquet.traits.{ExecutedTrait, LogTrait}
import org.apache.commons.cli.{BasicParser, OptionBuilder, Options}

/**
  * Created by fj on 16/11/20.
  */
class MsgProcExecutor extends ExecutedTrait with LogTrait {
  private var batchManager: MsgBatchManager = null

  case class Parameters(confValues: Properties, confFiles: Seq[String])

  override def execute(args: Array[String]): Unit = {
    //参数解析
    val parsed = parseCmd(args)
    if (!parsed._1) {
      println(parsed._3)
      System.exit(-1)
    }
    val parameters = parsed._2.get
    val confManager = new ConfManager(parameters.confValues, parameters.confFiles)
    batchManager = new MsgBatchManager()
    batchManager.init(confManager)
    try {
      batchManager.start()
    } catch {
      case e: Throwable => {
        LOG.error("batchManager start failure.", e)
        System.exit(-1)
      }
    }

  }

  override def shutdown(wait: Boolean = true): Unit = {
    batchManager.shutdown()
  }


  /**
    * 参数解析
    *
    * @param args
    * -f 逗号分隔的配置文件列表，默认为MsgBatchManager.xml
    * -c 以key=value形式指定配置键值对配置，value会覆盖掉配置文件中的#{key}
    * @return
    * _1:Boolean:解析是否成功
    * _2:Option[Parameters]: 解析的参数,如果解析失败则为None
    * _3:String:解析失败的描述
    */
  def parseCmd(args: Array[String]): (Boolean, Option[Parameters], String) = {
    val parser = new BasicParser
    val options = new Options

    //配置文件清单
    OptionBuilder.withLongOpt("confFiles")
    OptionBuilder.withDescription("逗号分隔的配置文件列表，默认为MsgBatchManager.xml")
    OptionBuilder.hasArg(true)
    val optS = OptionBuilder.create("f")
    optS.setRequired(false)
    options.addOption(optS)

    //c
    OptionBuilder.withLongOpt("conf")
    OptionBuilder.hasArgs(2)
    OptionBuilder.withValueSeparator()
    OptionBuilder.withDescription("以key=value形式指定配置键值对")
    val optC = OptionBuilder.create("c");
    optC.setRequired(false);
    options.addOption(optC)

    val cmdLine = parser.parse(options, args)


    val confValues = if (cmdLine.hasOption("c")) {
      cmdLine.getOptionProperties("c")
    } else {
      new Properties()
    }

    val confFiles = if (cmdLine.hasOption("f")) {
      cmdLine.getOptionValue("f").split(",")
    } else {
      Array("MsgBatchManager.xml")
    }
    val parameters = Parameters(confValues, confFiles)

    (true, Some(parameters), "")
  }
}
