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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.time.Duration;
import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.OFFSET_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;

/**
 * Native function returns committed offset for given partition.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getCommittedOffset",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {
                @Argument(name = "partition", type = TypeKind.RECORD, structType = TOPIC_PARTITION_STRUCT_NAME,
                        structPackage = KAFKA_NATIVE_PACKAGE),
                @Argument(name = "duration", type = TypeKind.INT)
        },
        returnType = {@ReturnType(type = TypeKind.RECORD,
                structPackage = KAFKA_NATIVE_PACKAGE, structType = OFFSET_STRUCT_NAME),
                @ReturnType(type = TypeKind.RECORD)},
        isPublic = true)
public class GetCommittedOffset extends AbstractApisWithDuration {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {

        setContext(context);
        BMap<String, BValue> consumerStruct = (BMap<String, BValue>) context.getRefArgument(0);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct.getNativeData(NATIVE_CONSUMER);

        if (Objects.isNull(kafkaConsumer)) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        long apiTimeout = context.getIntArgument(0);
        long defaultApiTimeout = getDefaultApiTimeout(consumerStruct);

        BMap<String, BValue> partition = (BMap<String, BValue>) context.getRefArgument(1);
        String topic = partition.get("topic").stringValue();
        int partitionValue = ((BInteger) partition.get("partition")).value().intValue();

        try {

            OffsetAndMetadata offsetAndMetadata;
            if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                Duration duration = getDurationFromLong(apiTimeout);
                offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(topic, partitionValue), duration);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                Duration duration = getDurationFromLong(defaultApiTimeout);
                offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(topic, partitionValue), duration);
            } else {
                offsetAndMetadata = kafkaConsumer.committed(new TopicPartition(topic, partitionValue));
            }
            BMap<String, BValue> offset = KafkaUtils.createKafkaPackageStruct(context, OFFSET_STRUCT_NAME);
            offset.put("partition", partition.copy());

            if (Objects.nonNull(offsetAndMetadata)) {
                offset.put("offset", new BInteger(offsetAndMetadata.offset()));
            }
            context.setReturnValues(offset);
        } catch (KafkaException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
        }
    }
}


