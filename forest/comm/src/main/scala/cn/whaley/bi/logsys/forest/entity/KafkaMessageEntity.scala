package cn.whaley.bi.logsys.forest.entity

import kafka.message.MessageAndMetadata

/**
 * Created by fj on 16/11/17.
 */
case class KafkaMessageEntity(topic: String, messages: Seq[MessageAndMetadata[Array[Byte], Array[Byte]]]) {

}
