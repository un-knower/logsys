libs目录下：
* phoenix-4.10.0-HBase-1.2-thin-client.jar，给ods-origin-metadata项目使用【删除了org/slf4j目录】
* phoenix-4.10.0-HBase-1.2-thin-client-without-hadoop.jar， 给log2parquet项目使用 【删除了org/slf4j目录、org/apache/hadoop目录】


1. phoenix-4.10.0-HBase-1.2-thin-client-without-hadoop.jar改造方式：
下载phoenix-4.10.0-HBase-1.2-thin-client.jar
因为，这个版本的hadoop和线上hadoop版本不一致，进一步会造成protobuf报错
解决方式：
需要解压jar包
删除org/apache/hadoop目录
删除org/slf4j目录
然后再次打包