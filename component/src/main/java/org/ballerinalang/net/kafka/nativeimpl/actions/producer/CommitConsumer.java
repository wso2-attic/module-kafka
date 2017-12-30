/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ballerinalang.net.kafka.nativeimpl.actions.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.BallerinaTransactionContext;
import org.ballerinalang.bre.BallerinaTransactionManager;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Native action ballerina.net.kafka:commitConsumer which commits the consumer offsets in transaction.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "commitConsumer",
        connectorName = Constants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "consumer",
                        type = TypeKind.STRUCT, structType = "KafkaConsumer",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class CommitConsumer extends AbstractNativeAction {

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector producerConnector = (BConnector) getRefArgument(context, 0);

        BStruct producerConf = ((BStruct) producerConnector.getRefField(0));
        BMap<String, BValue> producerBalConfig = (BMap<String, BValue>) producerConf.getRefField(0);

        BMap producerMap = (BMap) producerConnector.getRefField(1);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(Constants.NATIVE_PRODUCER));

        KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct
                .getNativeData(Constants.NATIVE_PRODUCER);

        BStruct consumerStruct = (BStruct) getRefArgument(context, 1);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(Constants.NATIVE_CONSUMER);

        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
        Set<TopicPartition> topicPartitions = kafkaConsumer.assignment();

        topicPartitions.forEach(tp -> {
            long pos = kafkaConsumer.position(tp);
            partitionToMetadataMap.put(new TopicPartition(tp.topic(), tp.partition()), new OffsetAndMetadata(pos));
        });

        BMap<String, BValue> consumerBalConfig = (BMap<String, BValue>) consumerStruct.getRefField(0);
        String groupID = consumerBalConfig.get(ConsumerConfig.GROUP_ID_CONFIG).stringValue();

        try {
            if (producerBalConfig.get(Constants.PARAM_TRANSACTION_ID) != null
                    && context.isInTransaction()) {
                String transactionID = producerBalConfig.get(Constants.PARAM_TRANSACTION_ID).stringValue();
                BallerinaTransactionManager ballerinaTxManager = context.getBallerinaTransactionManager();
                BallerinaTransactionContext regTxContext = ballerinaTxManager.getTransactionContext(transactionID);
                if (regTxContext == null) {
                    KafkaTransactionContext txContext = new KafkaTransactionContext(kafkaProducer);
                    ballerinaTxManager.registerTransactionContext(transactionID, txContext);
                    kafkaProducer.beginTransaction();
                }
            }
            kafkaProducer.sendOffsetsToTransaction(partitionToMetadataMap, groupID);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send offsets to transaction. " + e.getMessage(), e, context);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}

