package org.ballerinalang.kafka.nativeimpl.producer.action;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.ballerinalang.util.transactions.BallerinaTransactionContext;
import org.ballerinalang.util.transactions.LocalTransactionInfo;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * {@code AbstractCommitConsumer} is the base class for commit consumers.
 */
public abstract class AbstractTransactionHandler implements NativeCallableUnit {

    protected Context context;
    protected KafkaProducer producer;

    public void commitConsumer(Properties producerProperties,
                               BMap<String, BValue> producerConnector,
                               Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap,
                               String groupID) {

        try {
            if (isTransactionalProducer(producerProperties)) {
                initiateTransaction(producerConnector);
            }
            producer.sendOffsetsToTransaction(partitionToMetadataMap, groupID);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send offsets to transaction. " + e.getMessage(), e, context);
        }
    }

    @Override
    public boolean isBlocking() {

        return false;
    }

    private void performTransaction(LocalTransactionInfo localTransactionInfo, String connectorKey) {

        if (!isKafkaTransactionInitiated(localTransactionInfo, connectorKey)) {
            KafkaTransactionContext txContext = new KafkaTransactionContext(producer);
            localTransactionInfo.registerTransactionContext(connectorKey, txContext);
            producer.beginTransaction();
        }
    }

    public void initiateTransaction(BMap<String, BValue> producerConnector) {

        String connectorKey = producerConnector.get("connectorID").stringValue();
        LocalTransactionInfo localTransactionInfo = context.getLocalTransactionInfo();
        performTransaction(localTransactionInfo, connectorKey);
    }

    public boolean isTransactionalProducer(Properties properties) {

        return Objects.nonNull(properties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) && context.isInTransaction();
    }

    public boolean isKafkaTransactionInitiated(LocalTransactionInfo localTransactionInfo, String connectorKey) {
        BallerinaTransactionContext blnTxContext = localTransactionInfo.getTransactionContext(connectorKey);
        return Objects.nonNull(blnTxContext);
    }
}
