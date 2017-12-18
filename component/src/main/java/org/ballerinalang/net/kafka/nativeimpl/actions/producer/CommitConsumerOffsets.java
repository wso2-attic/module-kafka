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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code }
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "commitConsumerOffsets",
        connectorName = Constants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "offsets", type = TypeKind.ARRAY, elementType = TypeKind.STRUCT,
                        structType = "Offset",
                        structPackage = "ballerina.net.kafka"),
                @Argument(name = "groupID", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class CommitConsumerOffsets extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(CommitConsumerOffsets.class);

    @Override
    public ConnectorFuture execute(Context context) {
        //TODO check transaction context
        //TODO distributed transaction
        BConnector producerConnector = (BConnector) getRefArgument(context, 0);

        BMap producerMap = (BMap) producerConnector.getRefField(1);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(Constants.NATIVE_PRODUCER));

        KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct
                .getNativeData(Constants.NATIVE_PRODUCER);
        BRefValueArray offsets = ((BRefValueArray) getRefArgument(context, 1));
        String groupID = getStringArgument(context, 0);
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();

//        public struct Offset {
//            TopicPartition partition;
//            int offset;
//        }
//        public struct TopicPartition {
//            string topic;
//            int partition;
//        }

        for (int counter = 0; counter < offsets.size(); counter++) {
            BStruct offset = (BStruct) offsets.get(counter);
            BStruct partition = (BStruct) offset.getRefField(0);
            int offsetValue = new Long(offset.getIntField(0)).intValue();
            String topic = partition.getStringField(0);
            int partitionValue = new Long(partition.getIntField(0)).intValue();
            partitionToMetadataMap.put(new TopicPartition(topic, partitionValue), new OffsetAndMetadata(offsetValue));
        }

        try {
            kafkaProducer.sendOffsetsToTransaction(partitionToMetadataMap, groupID);
        } catch (KafkaException e) {
            throw new BallerinaException("Failed to send offsets to transaction. " + e.getMessage(), e, context);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}

