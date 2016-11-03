package cn.whaley.bi

import java.io.{FileInputStream, File}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkEnv, SparkFiles}

/**
 * Created by fj on 16/9/29.
 *
 * 定义公用的环境变量常量
 */
object ENV {
    val res_file_settings = "settings.properties"
    val prop_name_for_ip_isp = "com.moretv.bi.util.ip_isp"
    val prop_name_for_ip_country = "com.moretv.bi.util.ip_country"
    val prop_name_for_db_medusa_url = "com.moretv.bi.db.medusa.url"
    val prop_name_for_db_medusa_user = "com.moretv.bi.db.medusa.user"
    val prop_name_for_db_medusa_password = "com.moretv.bi.db.medusa.password"
    val prop_name_for_mtvaccount_snapshot_file_path = "com.moretv.bi.medusa.mtvaccount_snapshot_file_path"

    val resource_name_for_notOTT = "not_ott.dat"

    private lazy val conf = new Configuration(false)

    /**
     * 应用配置
     */
    val appConf: Configuration = {
        conf
    }

    /**
     * 往应用配置中增加配置资源
     * 先加载本地工作目录文件,然后加载根类路径上的资源文件
     * @param resources
     */
    def addConfResource(resources: String*): Unit = {
        resources.foreach(resource => {
            val ret = getSparkFileOrResourceAsStream(resource)
            val isOK = ret._1
            val sourceType = ret._2
            val stream = ret._3
            if (isOK) {
                conf.addResource(stream)
                println(s"add resource ${if (sourceType == 0) "file" else "object"}:$resource")
            } else {
                println(s"add resource failure, sparkRootDir:${SparkFiles.getRootDirectory()}")
                new File(SparkFiles.getRootDirectory()).listFiles().foreach(println(_))
            }
        })
    }

    /**
     * 从sparkFile或类路径资源列表中读取指定资源
     * @param name
     * @return
     * Boolean: 是否读取成功
     * Int: 读取途径,0=SparkFile, 1=类路径资源列表, -1:未找到资源
     * InputStream: 从资源对象构建的InputStream实例
     */
    def getSparkFileOrResourceAsStream(name: String): (Boolean, Int, java.io.InputStream) = {
        var filePath: String = null
        try {
            filePath = SparkFiles.get(name)
        } catch {
            case e: Throwable => println(e.getMessage)
        }
        if (filePath != null && new File(filePath).exists()) {
            (true, 0, new FileInputStream(filePath))
        } else {
            val stream = this.getClass.getClassLoader.getResourceAsStream(name)
            if (stream != null) {
                (true, 1, stream)
            } else {
                (false, -1, null)
            }
        }
    }

    /**
     * 从spark运行环境中获取文件路径
     * @param fileName
     * @return
     */
    def getSparkEnvFile(fileName: String): String = {
        s"${SparkFiles.get(fileName)}"
    }

    /**
     * 获取Spark工作环境根目录文件列表
     * @return
     */
    def getSparkEnvFileList(): Seq[String] = {
        val rootDir = SparkFiles.getRootDirectory()
        new File(rootDir).list()
    }

    /**
     * 上传Spark本地目录文件到HDFS上的Spark工作目录
     * @param localfilePath
     * @param fs
     * @return 上传后的HDFS文件全路径
     */
    def uploadSparkFileToHdfs(localfilePath: String, fs: FileSystem): String = {
        val fileName = new File(localfilePath).getName
        val hdfsPath = s"${SparkFiles.getRootDirectory()}/${fileName}"
        fs.copyFromLocalFile(false, true, new Path(localfilePath), new Path(hdfsPath))
        hdfsPath
    }

}
