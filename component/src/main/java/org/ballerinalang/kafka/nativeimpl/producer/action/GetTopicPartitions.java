/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.util.ArrayList;
import java.util.List;

import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_PARTITION;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native action retrieves partitions for given Topic via remote call.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getTopicPartitions",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE)
)
public class GetTopicPartitions implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BMap<String, BValue> producerConnector = (BMap<String, BValue>) context.getRefArgument(0);
        String topic = context.getStringArgument(0);

        BMap producerMap = (BMap) producerConnector.get("producerHolder");
        BMap<String, BValue> producerStruct = (BMap<String, BValue>) producerMap.get(new BString(NATIVE_PRODUCER));

        try {
            KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);
            List<PartitionInfo> partitionInfos = kafkaProducer.partitionsFor(topic);
            List<BMap<String, BValue>> infoList = new ArrayList<>();
            if (!partitionInfos.isEmpty()) {
                partitionInfos.forEach(partitionInfo -> {
                    BMap<String, BValue> infoStruct = getPartitionInfoStruct(context, partitionInfo);
                    infoList.add(infoStruct);
                });
                context.setReturnValues(
                        new BValueArray(infoList.toArray(new BRefType[0]),
                                KafkaUtils.createKafkaPackageStruct(
                                        context, TOPIC_PARTITION_STRUCT_NAME).getType()));
            }
        } catch (KafkaException e) {
            context.setReturnValues(
                    createError(context, "Failed to fetch partitions from the producer " + e.getMessage())
            );
        }
        callableUnitCallback.notifySuccess();
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    private BMap<String, BValue> getPartitionInfoStruct(Context context, PartitionInfo partitionInfo) {
        BMap<String, BValue> infoStruct = KafkaUtils.createKafkaPackageStruct(context, TOPIC_PARTITION_STRUCT_NAME);
        infoStruct.put("topic", new BString(partitionInfo.topic()));
        infoStruct.put(ALIAS_PARTITION, new BInteger(partitionInfo.partition()));
        return infoStruct;
    }
}
