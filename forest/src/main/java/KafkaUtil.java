import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.network.BlockingChannel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by fj on 15/9/20.
 */
public class KafkaUtil {

    private static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    private static final String defaultPropResrouceName = "kafka.properties";

    /**
     * 设置offset值,并返回设置之前的offset值
     *
     * @param host      用于初始化连接的broker主机地址,默认取资源文件[filepublisher.receiver.properties]中bootstrap.servers|metadata.broker.list中的第一个值
     * @param port      用于初始化连接的broker主机端口,默认取资源文件[filepublisher.receiver.properties]中bootstrap.servers|metadata.broker.list中的第一个值
     * @param topic     默认取资源文件[filepublisher.receiver.properties]中proddoc.kafka.topic.filereceived的值
     * @param groupID   默认取资源文件[filepublisher.receiver.properties]中group.id的值
     * @param clientID  默认为本机hostname
     * @param partition 默认为0
     * @param offset    指定的offset值
     * @return
     */
    public static void setOffset(String host, Integer port, String topic, String groupID, String clientID, Integer partition, Long offset) {

        if (offset == null || offset < 0) {
            logger.error("offset must be a larged than zero");
            return;
        }

        BlockingChannel channel = new BlockingChannel(host, port, BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
        channel.connect();

        logger.info("connect broker: {}:{}", host, port);

        int corId = 0;
        // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        short version = ConsumerMetadataRequest.CurrentVersion();

        ConsumerMetadataRequest request = new ConsumerMetadataRequest(groupID, version, corId++, clientID);
        channel.send(request);
        ConsumerMetadataResponse response = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
        if (response.errorCode() == ErrorMapping.NoError()) {
            Broker offsetManager = response.coordinator();
            channel.disconnect();
            channel = new BlockingChannel(offsetManager.host(), offsetManager.port()
                    , BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
            channel.connect();
        } else {
            logger.error("failure to get coordinator broker.");
            channel.disconnect();
            return;
        }

        long now = System.currentTimeMillis();
        TopicAndPartition tp = new TopicAndPartition(topic, partition);

        //set offset
        logger.info("set offset: {},{},{},{},{},{},{}", host, port, topic, groupID, clientID, partition, offset);
        Map<TopicAndPartition, OffsetAndMetadata> offsetMap = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();
        offsetMap.put(tp, new OffsetAndMetadata(offset, "set offset", now));
        OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(groupID, offsetMap, corId++, clientID, version);
        channel.send(offsetCommitRequest.underlying());
        OffsetCommitResponse offsetCommitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
        if (offsetCommitResponse.hasError()) {
            for (Object error : offsetCommitResponse.errors().values()) {
                if (error.equals(ErrorMapping.OffsetMetadataTooLargeCode())) {
                    logger.error("set offset[{}] error:OffsetMetadataTooLargeCode", offset);
                } else if (error.equals(ErrorMapping.NotCoordinatorForConsumerCode())) {
                    logger.error("set offset error:NotCoordinatorForConsumerCode");
                } else if (error.equals(ErrorMapping.ConsumerCoordinatorNotAvailableCode())) {
                    logger.error("set offset error:ConsumerCoordinatorNotAvailableCode");
                } else if (error.equals(ErrorMapping.UnknownTopicOrPartitionCode())) {
                    logger.error("set offset error:UnknownTopicOrPartitionCode");
                } else {
                    logger.error("offset commit error: {}", error);
                }
            }
        }
        channel.disconnect();
    }


    /**
     * 设置offset值,并返回设置之前的offset值
     *
     * @param host      用于初始化连接的broker主机地址,默认取资源文件[filepublisher.receiver.properties]中bootstrap.servers|metadata.broker.list中的第一个值
     * @param port      用于初始化连接的broker主机端口,默认取资源文件[filepublisher.receiver.properties]中bootstrap.servers|metadata.broker.list中的第一个值
     * @param topic     默认取资源文件[filepublisher.receiver.properties]中proddoc.kafka.topic.filereceived的值
     * @param groupID   默认取资源文件[filepublisher.receiver.properties]中group.id的值
     * @param clientID  默认为本机hostname
     * @param partition 默认为0
     * @return
     */
    public static Long getOffset(String host, Integer port, String topic, String groupID, String clientID, Integer partition) {

        Long preOffset = -1L;

        BlockingChannel channel = new BlockingChannel(host, port, BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
        channel.connect();

        logger.info("connect broker: {}:{}", host, port);

        int corId = 0;
        // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        short version = ConsumerMetadataRequest.CurrentVersion();

        ConsumerMetadataRequest request = new ConsumerMetadataRequest(groupID, version, corId++, clientID);
        channel.send(request);
        ConsumerMetadataResponse response = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
        if (response.errorCode() == ErrorMapping.NoError()) {
            Broker offsetManager = response.coordinator();
            channel.disconnect();
            channel = new BlockingChannel(offsetManager.host(), offsetManager.port()
                    , BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
            channel.connect();
        } else {
            logger.error("failure to get coordinator broker.");
            channel.disconnect();
            return preOffset;
        }

        TopicAndPartition tp = new TopicAndPartition(topic, partition);

        //get offset
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        partitions.add(tp);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                groupID,
                partitions,
                version,
                corId,
                clientID);

        channel.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
        OffsetMetadataAndError result = fetchResponse.offsets().get(tp);
        short offsetFetchErrorCode = result.error();
        if (offsetFetchErrorCode == ErrorMapping.NoError()) {
            preOffset = result.offset();
            String retrievedMetadata = result.metadata();
            logger.info("get current offset: {} ,{}", preOffset, retrievedMetadata);
        } else if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
            logger.error("get offset error:NotCoordinatorForConsumerCode");
        } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
            logger.error("get offset error:OffsetsLoadInProgressCode");
        } else if (offsetFetchErrorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
            logger.error("get offset error:UnknownTopicOrPartitionCode");
        } else {
            logger.error("get offset error:{}", offsetFetchErrorCode);
        }

        channel.disconnect();
        return preOffset;

    }

    /**
     * 从特定资源文件中定义的kafka属性列表来创建KafkaProducer对象,这些属性以kafka.开头,并在去掉kafka.的前缀作为最终的kafka属性
     *
     * @param loader       加载资源的classloader,如果为null,则为当前类加载器
     * @param resourceName 资源名称,如果为空,则试图查找kafka.properties,如果仍未发现,则继续查找kafka.properties环境变量值
     * @return 如果创建失败, 返回null
     */
    public static KafkaProducer createKafkaProducer(ClassLoader loader, String resourceName) {
        return createKafkaProducer(loader, resourceName, null);
    }

    public static KafkaProducer createKafkaProducer(ClassLoader loader, String resourceName, Map<String, Object> ps) {
        resourceName = resolveKafkaPropResrouceName(loader, resourceName);
        Properties properties = new Properties();
        try {
            properties.load(loader.getResourceAsStream(resourceName));
            Properties kafkaProps = new Properties();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String key = entry.getKey().toString();
                if (key.startsWith("kafka.")) {
                    kafkaProps.put(key.substring("kafka.".length()), entry.getValue());
                }
            }
            if (ps != null) {
                kafkaProps.putAll(ps);
            }
            KafkaProducer kafkaProducer = new KafkaProducer(kafkaProps);
            logger.info("kafkaProducer created. resource_name:{},class_loader:{}", resourceName, loader.getClass().getName());
            return kafkaProducer;
        } catch (IOException e) {
            logger.error("create KafkaProducer error", e);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 从特定资源文件中定义的kafka属性列表来创建ConsumerConnector对象,这些属性以kafka.开头,并在去掉kafka.的前缀作为最终的kafka属性
     *
     * @param loader       加载资源的classloader,如果为null,则为当前类加载器
     * @param resourceName 资源名称,如果为空,则试图查找kafka.properties,如果仍未发现,则继续查找kafka.properties环境变量值
     * @return 如果创建失败, 返回null
     */
    public static ConsumerConnector createConsumerConnector(ClassLoader loader, String resourceName) {
        return createConsumerConnector(loader, resourceName, null);
    }

    public static ConsumerConnector createConsumerConnector(ClassLoader loader, String resourceName, Map<String, Object> ps) {
        resourceName = resolveKafkaPropResrouceName(loader, resourceName);
        Properties properties = new Properties();
        try {
            properties.load(loader.getResourceAsStream(resourceName));
            Properties kafkaProps = new Properties();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String key = entry.getKey().toString();
                if (key.startsWith("kafka.")) {
                    kafkaProps.put(key.substring("kafka.".length()), entry.getValue());
                }
            }
            if (ps != null) {
                kafkaProps.putAll(ps);
            }
            ConsumerConnector kafkaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaProps));
            return kafkaConsumerConnector;
        } catch (IOException e) {
            logger.error("create ConsumerConnector error", e);
            return null;
        }
    }

    /**
     * 解析kafka属性资源文件名
     *
     * @param loader
     * @param resourceName
     * @return
     */
    private static String resolveKafkaPropResrouceName(ClassLoader loader, String resourceName) {
        if (loader == null) {
            loader = KafkaUtil.class.getClassLoader();
        }
        if (resourceName == null || resourceName.trim().length() == 0) {
            if (loader.getResource(defaultPropResrouceName) != null) {
                resourceName = defaultPropResrouceName;
            } else {
                String env = System.getenv(defaultPropResrouceName);
                if (env != null && env.trim().length() > 0) {
                    resourceName = env;
                }
            }
        }
        return resourceName;
    }

}
