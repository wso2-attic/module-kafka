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

package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native function returns partition array for given topic.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getTopicPartitions",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true
)
//Duplicated code here cannot be moved as the Collection<T> has different types
@SuppressWarnings("Duplicates")
public class GetTopicPartitions extends AbstractApisWithDuration {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        this.consumer = getKafkaConsumer();
        String topic = context.getStringArgument(0);
        long apiTimeout = context.getIntArgument(0);
        long defaultApiTimeout = getDefaultApiTimeout();

        try {
            List<PartitionInfo> partitionInfoList;
            if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                partitionInfoList = getPartitionInfoList(topic, apiTimeout);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                partitionInfoList = getPartitionInfoList(topic, defaultApiTimeout);
            } else {
                partitionInfoList = this.consumer.partitionsFor(topic);
            }
            List<BMap<String, BValue>> infoList = getInfoList(partitionInfoList);
            context.setReturnValues(
                    new BValueArray(
                            infoList.toArray(new BRefType[0]),
                            KafkaUtils.
                                    createKafkaPackageStruct(context, TOPIC_PARTITION_STRUCT_NAME).getType()));
        } catch (KafkaException e) {
            context.setReturnValues(createError(context, e.getMessage()));
        }
    }

    private List<PartitionInfo> getPartitionInfoList(String topic, long timeout) {
        Duration duration = getDurationFromLong(timeout);
        return this.consumer.partitionsFor(topic, duration);
    }

    private List<BMap<String, BValue>> getInfoList(List<PartitionInfo> partitionInfoList) {
        List<BMap<String, BValue>> infoList = new ArrayList<>();
        if (!partitionInfoList.isEmpty()) {
            partitionInfoList.forEach(partitionInfo -> {
                BMap<String, BValue> partitionStruct = KafkaUtils
                        .createKafkaPackageStruct(this.context, TOPIC_PARTITION_STRUCT_NAME);
                partitionStruct.put("topic", new BString(partitionInfo.topic()));
                partitionStruct.put("partition", new BInteger(partitionInfo.partition()));
                infoList.add(partitionStruct);
            });
        }
        return infoList;
    }
}
