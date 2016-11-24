package cn.whaley.bi.logsys.common

import java.io.{OutputStream, FileInputStream}
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._

/**
 * Created by fj on 16/10/30.
 *
 * 配置管理类，加载以hadoop风格的配置xml或properties文件
 * 可以通过key获取value值，或通过key前缀获取一批value值，且其中的#{XXX}部分被替换为在props中的XXX对应的value值
 *
 * @param props 其中的键值对用于替换获取到的配置值中的#{...}部分
 * @param resources xml文件或properties文件，其他文件将被忽略
 * @param classLoader 资源文件对应的ClassLoader，默认为当前类实例对应的系统类加载器
 */
class ConfManager(props: Properties, resources: Seq[String], classLoader: ClassLoader) {

    def this(prop: Properties, resources: Seq[String]) {
        this(prop, resources, ConfManager.classLoader)
    }

    def this(resources: Seq[String]) {
        this(ConfManager.EMPTY_PROP, resources, ConfManager.classLoader)
    }


    val conf = new Configuration(false)

    {
        addConfResource(resources: _*)
    }

    /**
     * 将配置写入到外部流
     * @param out
     */
    def writeXml(out: OutputStream): Unit = {
        conf.writeXml(out)
    }

    /**
     * 往应用配置中增加配置资源
     * 先加载本地工作目录文件,然后加载根类路径上的资源文件
     * @param resources
     */
    def addConfResource(resources: String*): Unit = {
        resources.filter(item => item.endsWith("properties")).foreach(item => {
            val props = new Properties()
            val stream = ConfManager.getResourceAsStream(item)
            if (stream != null) {
                props.load(stream)
                val el = props.propertyNames()
                while (el.hasMoreElements) {
                    val propName = el.nextElement().toString
                    conf.set(propName, props.getProperty(propName))
                }
                println(s"load resource ${item}")
            } else {
                println(s"can not load resource ${item}")
            }
        })

        resources.filter(item => item.endsWith(".xml")).foreach(item => {
            val stream = ConfManager.getResourceAsStream(item)
            if (stream != null) {
                conf.addResource(stream)
                println(s"load resource ${item}")
            } else {
                println(s"can not load resource ${item}")
            }
        })
    }

    /**
     * 设置配置值
     * @param prefix
     * @param confKey
     * @param value
     */
    def putConf(prefix: String, confKey: String, value: String): Unit = {
        conf.set(s"${prefix}.${confKey}", value)
    }

    /**
     * 设置配置值
     * @param confKey
     * @param value
     */
    def putConf(confKey: String, value: String): Unit = {
        conf.set(s"${confKey}", value)
    }

    /**
     * 设置配置值
     * @param confs
     */
    def putAllConf(confs: Map[String, String]): Unit = {
        confs.map(item => {
            conf.set(item._1, item._2)
        })
    }

    /**
     * 设置配置值
     * @param confs
     */
    def putAllConf(confs: Properties): Unit = {
        val it = confs.propertyNames()
        while (it.hasMoreElements) {
            val curr = it.nextElement().asInstanceOf[String]
            conf.set(curr, confs.getProperty(curr))
        }
    }


    /**
     * 获取所有以prefix为前缀的配置
     * @param prefix
     * @return
     * key: 完整的配置key
     * value: 配置值
     */
    def getAllConf(prefix: String, removePrefix: Boolean): Properties = {
        val itr = conf.iterator()
        val properties = new Properties()
        while (itr.hasNext) {
            val next = itr.next()
            if (next.getKey.startsWith(s"${prefix}.")) {
                val confKey = if (removePrefix) {
                    next.getKey.substring(prefix.length + 1)
                } else {
                    next.getKey
                }
                val confValue = this.conf.get(next.getKey)
                properties.put(confKey, confValue)
            }
        }
        properties
    }

    /**
     * 获取所有配置
     * @return
     * key: 完整的配置key
     * value: 配置值
     */
    def getAllConf(): Properties = {
        val itr = conf.iterator()
        val properties = new Properties()
        while (itr.hasNext) {
            val next = itr.next()
            val confKey = next.getKey
            val confValue = this.getConf(confKey)
            properties.put(confKey, confValue)
        }
        properties
    }

    /**
     * 获取配置值
     * @param confKey
     * @return
     */
    def getConf(confKey: String): String = {
        val value = conf.get(confKey)
        if (value == null || value.trim == "") {
            value
        } else {
            resolveConf(value)
        }
    }

    /**
     * 获取配置值
     * @param prefix
     * @param confKey
     * @return
     */
    def getConf(prefix: String, confKey: String): String = {
        var value = if (prefix != null && prefix.trim.length > 0) conf.get(s"${prefix}.${confKey}") else confKey
        if (value == null || value.trim == "") {
            val parent = conf.get(s"${prefix}.parent")
            if (parent != null && parent.trim != "") {
                value = getConf(parent, confKey)
            }
        }
        resolveConf(value)
    }

    /**
     * 获取配置值,如果不存在则返回orConfKey的配置值
     * @param prefix
     * @param confKey
     * @return
     */
    def getConfOrElse(prefix: String, confKey: String, orConfKey: String): String = {
        var value = getConf(prefix, confKey)
        if (value == null || value.trim.isEmpty) {
            value = getConf(prefix, orConfKey)
        }
        value
    }

    /**
     * 获取配置值,如果不存在则返回orConfValue的配置值
     * @param prefix
     * @param confKey
     * @return
     */
    def getConfOrElseValue(prefix: String, confKey: String, orConfValue: String): String = {
        var value = getConf(prefix, confKey)
        if (value == null || value.trim.isEmpty) {
            value = orConfValue
        }
        value
    }

    /**
     * 从一个配置前缀派生出一个子配置管理器对象
     * @param prefix
     * @param removePrefix
     */
    def getSubConfManager(prefix: String, removePrefix: Boolean): ConfManager = {
        val confManager = new ConfManager(Array(""))
        val items = this.getAllConf(prefix, removePrefix)
        confManager.putAllConf(items)
        confManager
    }

    /**
     * 替换confValue中的#{propName}形式字符串为parameters.confs中propName对应的配置值
     * @param confValue
     * @return
     */
    private def resolveConf(confValue: String): String = {
        if (confValue != null) {
            var value = confValue

            //替换配置中的#{propName}为实际的配置值
            val reg = "#\\{(.*?)\\}".r
            reg.findAllIn(value).foreach(key => {
                val confKey = key.substring(2, key.length - 1)
                if (props.containsKey(confKey)) {
                    val confValue = props.getProperty(confKey)
                    value = value.replace(key, confValue)
                }
            })

            value
        } else {
            confValue
        }
    }

}

object ConfManager {
    private val EMPTY_PROP = new Properties()
    private val classLoader = ConfManager.getClass.getClassLoader

    /**
     * 从类路径资源列表中读取指定资源
     * @param resPath resource://{resourcePath};  file://{filePath}; /{filePath}; {resourcePath}
     * @return
     * InputStream: 从资源对象构建的InputStream实例,如果失败则返回null
     */
    def getResourceAsStream(resPath: String, loader: ClassLoader = ConfManager.classLoader): java.io.InputStream = {
        if (resPath.startsWith("resource://")) {
            loader.getResourceAsStream(resPath.substring("resource://".length))
        } else if (resPath.startsWith("file://")) {
            new FileInputStream(resPath.substring("file://".length))
        } else if (resPath.startsWith("/")) {
            new FileInputStream(resPath)
        } else {
            loader.getResourceAsStream(resPath)
        }
    }
}
