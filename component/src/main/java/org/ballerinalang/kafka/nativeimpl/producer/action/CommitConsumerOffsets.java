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

package org.ballerinalang.kafka.nativeimpl.producer.action;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER_CONFIG;
import static org.ballerinalang.kafka.util.KafkaConstants.OFFSET_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;

/**
 * Native action commits the consumer for given offsets in transaction.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "commitConsumerOffsets",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {
                @Argument(name = "offsets", type = TypeKind.ARRAY, elementType = TypeKind.RECORD,
                        structType = OFFSET_STRUCT_NAME, structPackage = KAFKA_NATIVE_PACKAGE),
                @Argument(name = "groupID", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class CommitConsumerOffsets extends AbstractCommitConsumer {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {

        setContext(context);
        BMap<String, BValue> producerConnector = (BMap<String, BValue>) context.getRefArgument(0);

        BMap producerMap = (BMap) producerConnector.get("producerHolder");
        BMap<String, BValue> producerStruct = (BMap<String, BValue>) producerMap.get(new BString(NATIVE_PRODUCER));

        KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);

        if (Objects.isNull(kafkaProducer)) {
            throw new BallerinaException("Kafka Producer has not been initialized properly.");
        }

        Properties producerProperties = (Properties) producerStruct.getNativeData(NATIVE_PRODUCER_CONFIG);

        BRefValueArray offsets = ((BRefValueArray) context.getRefArgument(1));
        String groupID = context.getStringArgument(0);
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();

        BMap<String, BValue> offset;
        BMap<String, BValue> partition;
        int offsetValue;
        String topic;
        int partitionValue;

        for (int counter = 0; counter < offsets.size(); counter++) {
            offset = (BMap<String, BValue>) offsets.get(counter);
            partition = (BMap<String, BValue>) offset.get("partition");
            offsetValue = ((BInteger) offset.get("offset")).value().intValue();
            topic = partition.get("topic").stringValue();
            partitionValue = ((BInteger) partition.get("partition")).value().intValue();
            partitionToMetadataMap.put(new TopicPartition(topic, partitionValue), new OffsetAndMetadata(offsetValue));
        }

        commitConsumer(producerProperties,
                producerConnector,
                kafkaProducer,
                partitionToMetadataMap,
                groupID);
        callableUnitCallback.notifySuccess();
    }
}

