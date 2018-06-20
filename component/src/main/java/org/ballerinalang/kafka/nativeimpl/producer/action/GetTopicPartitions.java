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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;

/**
 * Native action retrieves partitions for given Topic via remote call.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getTopicPartitions",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {
                @Argument(name = "topic", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.RECORD,
                structType = TOPIC_PARTITION_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE)})
public class GetTopicPartitions implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BStruct producerConnector = (BStruct) context.getRefArgument(0);
        String topic = context.getStringArgument(0);

        BMap producerMap = (BMap) producerConnector.getRefField(0);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(NATIVE_PRODUCER));

        try {
            KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);

            if (Objects.isNull(kafkaProducer)) {
                throw new BallerinaException("Kafka producer has not been initialized properly.");
            }

            List<PartitionInfo> partitionInfos = kafkaProducer.partitionsFor(topic);
            List<BStruct> infoList = new ArrayList<>();
            if (!partitionInfos.isEmpty()) {
                partitionInfos.forEach(partitionInfo -> {
                    BStruct infoStruct = KafkaUtils.
                            createKafkaPackageStruct(context, TOPIC_PARTITION_STRUCT_NAME);
                    infoStruct.setStringField(0, partitionInfo.topic());
                    infoStruct.setIntField(0, partitionInfo.partition());
                    infoList.add(infoStruct);
                });
                context.setReturnValues(
                        new BRefValueArray(infoList.toArray(new BRefType[0]),
                                           KafkaUtils.createKafkaPackageStruct(
                                                           context, TOPIC_PARTITION_STRUCT_NAME).getType()));
            }
        } catch (KafkaException e) {
            throw new BallerinaException("Failed to fetch partitions from the producer " + e.getMessage(), e, context);
        }
        callableUnitCallback.notifySuccess();
    }

    @Override
    public boolean isBlocking() {
        return false;
    }
}
