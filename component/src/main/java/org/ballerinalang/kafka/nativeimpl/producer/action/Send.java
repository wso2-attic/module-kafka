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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BByteArray;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Objects;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER_CONFIG;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;

/**
 * Native action produces blob value to given string topic.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "send",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE)
)
public class Send extends AbstractTransactionHandler {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        BMap<String, BValue> producerConnector = (BMap<String, BValue>) context.getRefArgument(0);

        BMap producerMap = (BMap) producerConnector.get("producerHolder");
        BMap<String, BValue> producerStruct = (BMap<String, BValue>) producerMap.get(new BString(NATIVE_PRODUCER));

        this.producer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);
        Properties producerProperties = (Properties) producerStruct.getNativeData(NATIVE_PRODUCER_CONFIG);

        String topic = context.getStringArgument(0);
        byte[] value = ((BByteArray) context.getRefArgument(1)).getBytes();
        BByteArray bKey = (BByteArray) context.getNullableRefArgument(2);
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

        if (Objects.isNull(producer) || Objects.isNull(kafkaRecord)) {
            throw new BallerinaException("Kafka producer/record has not been initialized properly.");
        }

        try {
            if (isTransactionalProducer(producerProperties)) {
                initiateTransaction(producerConnector);
            }
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (Objects.nonNull(exception)) {
                    throw new BallerinaException("Failed to send message. " +
                            exception.getMessage(), exception, context);
                }
                //kafkaProducer.flush();
                callableUnitCallback.notifySuccess();
            });
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send message. " + e.getMessage(), e, context);
        }
    }
}

