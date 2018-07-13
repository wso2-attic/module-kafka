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

package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Objects;
import java.util.regex.Pattern;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;

/**
 * Native function subscribes consumer to given topic pattern.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "subscribeToPattern",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {
                @Argument(name = "regex", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.RECORD)},
        isPublic = true)
public class SubscribeToPattern implements NativeCallableUnit {
    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BMap<String, BValue> consumerStruct = (BMap<String, BValue>) context.getRefArgument(0);
        String topicRegex = context.getStringArgument(0);

        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct.getNativeData(NATIVE_CONSUMER);

        if (Objects.isNull(kafkaConsumer)) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        try {
            kafkaConsumer.subscribe(Pattern.compile(topicRegex));
        } catch (IllegalArgumentException |
                IllegalStateException | KafkaException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

}

