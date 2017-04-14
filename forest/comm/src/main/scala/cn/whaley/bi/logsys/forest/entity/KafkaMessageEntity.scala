package cn.whaley.bi.logsys.forest.entity

import org.apache.kafka.clients.consumer.ConsumerRecord


/**
 * Created by fj on 16/11/17.
 */
case class KafkaMessageEntity(topic: String, messages: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]) {

}
