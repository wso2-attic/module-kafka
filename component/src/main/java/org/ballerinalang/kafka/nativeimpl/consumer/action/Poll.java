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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BByteArray;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_RECORD_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;

/**
 * Native function polls the broker to retrieve messages within given timeout.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "poll",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {
                @Argument(name = "timeout", type = TypeKind.INT)
        },
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.RECORD,
                structPackage = KAFKA_NATIVE_PACKAGE, structType = CONSUMER_RECORD_STRUCT_NAME),
                @ReturnType(type = TypeKind.RECORD)},
        isPublic = true)
public class Poll implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BMap<String, BValue> consumerStruct = (BMap<String, BValue>) context.getRefArgument(0);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct.getNativeData(NATIVE_CONSUMER);

        if (Objects.isNull(kafkaConsumer)) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        long timeout = context.getIntArgument(0);
        List<BMap<String, BValue>> recordsList = new ArrayList<>();
        Duration duration = Duration.ofMillis(timeout);

        try {
            ConsumerRecords<byte[], byte[]> recordsRetrieved = kafkaConsumer.poll(duration);
            if (!recordsRetrieved.isEmpty()) {
                recordsRetrieved.forEach(record -> {
                    BMap<String, BValue> recordStruct = KafkaUtils.
                            createKafkaPackageStruct(context, CONSUMER_RECORD_STRUCT_NAME);
                    if (record.key() != null) {
                        recordStruct.put("key", new BByteArray(record.key()));
                    }
                    recordStruct.put("value", new BByteArray(record.value()));
                    recordStruct.put("offset", new BInteger(record.offset()));
                    recordStruct.put("partition", new BInteger(record.partition()));
                    recordStruct.put("timestamp", new BInteger(record.timestamp()));
                    recordStruct.put("topic", new BString(record.topic()));
                    recordsList.add(recordStruct);
                });
            }
            context.setReturnValues(new BRefValueArray(recordsList.toArray(new BRefType[0]),
                                                 KafkaUtils.createKafkaPackageStruct(context,
                                                             CONSUMER_RECORD_STRUCT_NAME).getType()));
        } catch (IllegalStateException |
                IllegalArgumentException | KafkaException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}


