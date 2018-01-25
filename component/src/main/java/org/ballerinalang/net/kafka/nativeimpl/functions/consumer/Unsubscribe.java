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

package org.ballerinalang.net.kafka.nativeimpl.functions.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.util.exceptions.BallerinaException;

/**
 * Native function ballerina.net.kafka:unsubscribe un - subscribes consumer from current subscription.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "unsubscribe",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Consumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "Consumer",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = {@ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class Unsubscribe extends AbstractNativeFunction {

    @Override
    public BValue[] execute(Context context) {
        BStruct consumerStruct = (BStruct) getRefArgument(context, 0);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(KafkaConstants.NATIVE_CONSUMER);
        if (kafkaConsumer == null) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        try {
            kafkaConsumer.unsubscribe();
        } catch (KafkaException e) {
            return getBValues(BLangVMErrors.createError(context, 0, e.getMessage()));
        }
        return VOID_RETURN;
    }

}

