/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.ballerinalang.net.kafka.KafkaUtils;

import java.util.Properties;

/**
 * Native function ballerina.net.kafka:connect which connects consumer to remote cluster.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "connect",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Consumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "Consumer",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = {@ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class Connect extends AbstractNativeFunction {

    @Override
    public BValue[] execute(Context context) {
        // Consumer initialization.
        BStruct consumerStruct = (BStruct) getRefArgument(context, 0);
        BStruct consumerConfig = (BStruct) consumerStruct.getRefField(0);
        // Check whether consumer configuration is available.
        if (consumerConfig == null) {
            return getBValues(BLangVMErrors.createError(context,
                    0, "Kafka consumer is not initialized with consumer configuration."));
        }
        // Check whether already native consumer is attached to the struct.
        // This can be happen either from Kafka service or via programmatically.
        if (consumerStruct.getNativeData(KafkaConstants.NATIVE_CONSUMER) != null) {
            return getBValues(BLangVMErrors.createError(context,
                    0, "Kafka consumer is already connected to external broker." +
                            " Please close it before re-connecting the external broker again."));
        }

        Properties consumerProperties = KafkaUtils.processKafkaConsumerConfig(consumerConfig);

        try {
            KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
            consumerStruct.addNativeData(KafkaConstants.NATIVE_CONSUMER, kafkaConsumer);
        } catch (KafkaException e) {
            return getBValues(BLangVMErrors.createError(context, 0, e.getMessage()));
        }
        return VOID_RETURN;
    }

}

