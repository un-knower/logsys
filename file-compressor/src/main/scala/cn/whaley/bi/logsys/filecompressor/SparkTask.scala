package cn.whaley.bi.logsys.filecompressor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object SparkTask {
    /**
     *
    args: [genericOptions] [commandOptions]

     genericOptions:

     -conf <configuration file>     specify a configuration file
     -D <property=value>            use value for given property
     -fs <local|namenode:port>      specify a namenode
     -jt <local|jobtracker:port>    specify a job tracker
     -files <comma separated list of files>    specify comma separated
     files to be copied to the map reduce cluster
     -libjars <comma separated list of jars>   specify comma separated
     jar files to include in the classpath.
     -archives <comma separated list of archives>    specify comma
     separated archives to be unarchived on the compute machines.

     commandOptions:

     --cmd 指令, compress/decompress
     --srcPath: 输入路径
     --outPath: 输出路径
     --split: 分片数量
     --bufSize: 缓冲读的字节数
     --codec: Codec类名, BZip2Codec,GzipCodec,Lz4Codec,SnappyCodec

     * @param args
     * @throws Exception
     */

    def main(args: Array[String]): Unit = {
        val conf = new Configuration()
        val params: java.util.Map[String, String] = new java.util.HashMap[String, String]()
        val otherArgs: Array[String] = new GenericOptionsParser(conf, args).getRemainingArgs
        for (i <- 0 to otherArgs.length - 1) {
            val arg: String = args(i)
            if (arg.startsWith("--")) {
                val idx: Int = arg.indexOf('=')
                val key: String = arg.substring(2, idx)
                val value: String = arg.substring(idx + 1)
                params.put(key, value)
            }
        }

        val cmd: String = params.get("cmd")
        val srcPath: String = params.get("srcPath")
        val outPath: String = params.get("outPath")
        var bufSize: Integer = Compressor.DEFAULT_BUF_SIZE
        var split: Integer = 1
        if (params.containsKey("bufSize")) {
            bufSize = params.get("bufSize").toInt
        }
        if (params.containsKey("split")) {
            split = params.get("split").toInt
        }
        if (cmd.equalsIgnoreCase("compress")) {
            val codec: String = "org.apache.hadoop.io.compress." + params.get("codec")
            startCompressTask(conf, srcPath, codec, outPath, split, bufSize)
        }
        else if (cmd.equalsIgnoreCase("decompress")) {
            startDecompressTask(conf, srcPath, outPath, split, bufSize)
        }
        else {
            System.err.println("Error!\n invalid cmd " + cmd)
            System.err.println("args: [genericOptions] --cmd=.. --srcPath=.. --outPath=.. --bufSize=.. --split=..")
            return
        }
        System.out.println("down")

    }

    def startCompressTask(conf: Configuration, srcPath: String, codec: String, outPath: String, split: Int, bufSize: Int): Unit = {

        val confMap = confToMap(conf)
        val fs = FileSystem.get(conf)
        val statuses = fs.globStatus(new Path(srcPath))
        val srcFilePaths = statuses.filter(_.isFile).filter(_.getLen > 0).map(status => {
            val fileName = status.getPath.getName
            val withoutExt = fileName.substring(0, fileName.lastIndexOf('.'))
            (withoutExt, status.getPath.toUri.getPath)
        })

        val sparkConf = new SparkConf()
        val context = new SparkContext(sparkConf)
        val rdd = context.makeRDD(srcFilePaths)

        println(s"srcFilePaths[${srcFilePaths.length}]:\n${srcFilePaths.mkString("\n")}")

        val ret = rdd.partitionBy(new TaskPartitioner(srcFilePaths.map(_._1)))
            .repartition(srcFilePaths.length)
            .map(taskPath => {
            val taskConf = confFromMap(confMap)
            val outFilePath = outPath + "/" + taskPath._1
            val ts = System.currentTimeMillis()
            Compressor.compress(taskConf, taskPath._2, codec, outFilePath, split, bufSize)
            (System.currentTimeMillis() - ts, taskPath._2, outFilePath)
        }).collect()

        println(ret.mkString("\n"))

    }

    def startDecompressTask(conf: Configuration, srcPath: String, outPath: String, split: Int, bufSize: Int): Unit = {

        val confMap = confToMap(conf)
        val fs = FileSystem.get(conf)
        val statuses = fs.globStatus(new Path(srcPath))
        val srcFilePaths = statuses.filter(_.isFile).filter(_.getLen > 0).map(status => {
            (status.getPath.getName, status.getPath.toUri.getPath)
        })

        val sparkConf = new SparkConf()
        val context = new SparkContext(sparkConf)
        val rdd = context.makeRDD(srcFilePaths)

        rdd.foreach(taskPath => {
            val taskConf = confFromMap(confMap)
            val outFilePath = outPath + "/" + taskPath._1
            Compressor.decompress(taskConf, taskPath._2, outFilePath, bufSize)
        })

    }

    def confToMap(conf: Configuration): Map[String, String] = {
        val it = conf.iterator();
        val map = new mutable.HashMap[String, String]()
        while (it.hasNext) {
            val next = it.next()
            map.put(next.getKey, next.getValue)
        }
        map.toMap
    }

    def confFromMap(map: Map[String, String]): Configuration = {
        val conf = new Configuration()
        map.foreach(item => {
            conf.set(item._1, item._2);
        })
        conf
    }

    /**
     * 每个key对应一个分区
     * @param keys
     */
    class TaskPartitioner(keys: Array[String]) extends Partitioner {
        override def numPartitions: Int = keys.length

        override def getPartition(key: Any): Int = {
            val id = key match {
                case null => 0
                case _: String => {
                    var partId = nonNegativeMod(key.hashCode, numPartitions)
                    for (i <- 0 to keys.length - 1) {
                        if (keys(i) == key) {
                            partId = i
                        }
                    }
                    partId
                }
            }
            println(s"partitionId: ${key} -> ${id}")
            id
        }

        private def nonNegativeMod(x: Int, mod: Int): Int = {
            val rawMod = x % mod
            rawMod + (if (rawMod < 0) mod else 0)
        }
    }

}
