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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.FULL_PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native action produces blob value to given string topic.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = FULL_PACKAGE_NAME,
        functionName = "send",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE)
)
public class Send extends AbstractTransactionHandler {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        initializeClassVariables();
        String topic = context.getStringArgument(0);
        byte[] value = ((BValueArray) context.getRefArgument(1)).getBytes();
        BValueArray bKey = (BValueArray) context.getNullableRefArgument(2);
        BInteger bPartition = (BInteger) context.getNullableRefArgument(3);
        BInteger bTimestamp = (BInteger) context.getNullableRefArgument(4);
        byte[] key = Objects.nonNull(bKey) ? bKey.getBytes() : null;
        Long partition = Objects.nonNull(bPartition) ? bPartition.value() : null;
        Long timestamp = Objects.nonNull(bTimestamp) ? bTimestamp.value() : null;
        ProducerRecord<byte[], byte[]> kafkaRecord;
        if (Objects.nonNull(partition)) {
            kafkaRecord = new ProducerRecord(topic, partition.intValue(), timestamp, key, value);
        } else {
            kafkaRecord = new ProducerRecord(topic, null, timestamp, key, value);
        }
        try {
            if (isTransactionalProducer()) {
                initiateTransaction();
            }
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (Objects.nonNull(exception)) {
                    context.setReturnValues(createError(
                            context, "Failed to send message. " + exception.getMessage())
                    );
                }
                callableUnitCallback.notifySuccess();
            });
            context.setReturnValues();
        } catch (IllegalStateException | KafkaException e) {
            context.setReturnValues(createError(context, "Failed to send message." + e.getMessage()));
        }
    }
}
