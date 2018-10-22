package org.ballerinalang.kafka.nativeimpl.producer.action;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Map;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaUtils.beginTransaction;
import static org.ballerinalang.kafka.util.KafkaUtils.isTransactionalProducer;

public abstract class AbstractCommitConsumer implements NativeCallableUnit {

    public static void commitConsumer(Context context,
                                       Properties producerProperties,
                                       BMap<String, BValue> producerConnector,
                                       KafkaProducer kafkaProducer,
                                       Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap,
                                       String groupID) {
        try {
            if (isTransactionalProducer(context, producerProperties)) {
                beginTransaction(context, producerConnector, kafkaProducer);
            }
            kafkaProducer.sendOffsetsToTransaction(partitionToMetadataMap, groupID);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send offsets to transaction. " + e.getMessage(), e, context);
        }
    }
}
