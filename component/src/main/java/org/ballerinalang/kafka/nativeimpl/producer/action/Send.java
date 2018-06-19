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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BBlob;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.ballerinalang.util.transactions.BallerinaTransactionContext;
import org.ballerinalang.util.transactions.LocalTransactionInfo;

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
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {
                @Argument(name = "value", type = TypeKind.BLOB),
                @Argument(name = "topic", type = TypeKind.STRING),
                @Argument(name = "key", type = TypeKind.UNION),
                @Argument(name = "partition", type = TypeKind.UNION),
                @Argument(name = "timestamp", type = TypeKind.UNION)
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class Send implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BStruct producerConnector = (BStruct) context.getRefArgument(0);

        BMap producerMap = (BMap) producerConnector.getRefField(0);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(NATIVE_PRODUCER));

        KafkaProducer kafkaProducer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);
        Properties producerProperties = (Properties) producerStruct.getNativeData(NATIVE_PRODUCER_CONFIG);

        String topic = context.getStringArgument(0);
        byte[] value = context.getBlobArgument(0);
        BBlob bKey = (BBlob) context.getNullableRefArgument(1);
        BInteger bPartition = (BInteger) context.getNullableRefArgument(2);
        BInteger bTimestamp = (BInteger) context.getNullableRefArgument(3);

        byte[] key = Objects.nonNull(bKey) ? bKey.blobValue() : null;
        Long partition = Objects.nonNull(bPartition) ? bPartition.value() : null;
        Long timestamp = Objects.nonNull(bTimestamp) ? bTimestamp.value() : null;

        ProducerRecord<byte[], byte[]> kafkaRecord;
        if (Objects.nonNull(partition)) {
            kafkaRecord = new ProducerRecord(topic, partition.intValue(), timestamp, key, value);
        } else {
            kafkaRecord = new ProducerRecord(topic, null, timestamp, key, value);
        }

        if (Objects.isNull(kafkaProducer) || Objects.isNull(kafkaRecord)) {
            throw new BallerinaException("Kafka producer/record has not been initialized properly.");
        }

        try {
            if (Objects.nonNull(producerProperties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
                        && context.isInTransaction()) {
                String connectorKey = producerConnector.getStringField(0);
                LocalTransactionInfo localTransactionInfo = context.getLocalTransactionInfo();
                BallerinaTransactionContext regTxContext = localTransactionInfo.getTransactionContext(connectorKey);
                if (Objects.isNull(regTxContext)) {
                    KafkaTransactionContext txContext = new KafkaTransactionContext(kafkaProducer);
                    localTransactionInfo.registerTransactionContext(connectorKey, txContext);
                    kafkaProducer.beginTransaction();
                }
            }
            kafkaProducer.send(kafkaRecord, (metadata, exception) -> {
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

    @Override
    public boolean isBlocking() {
        return false;
    }
}

