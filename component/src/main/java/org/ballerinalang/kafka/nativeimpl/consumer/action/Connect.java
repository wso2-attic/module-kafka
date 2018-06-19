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

package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.util.Objects;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;

/**
 * Native function connects consumer to remote cluster.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "connect",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true)
public class Connect implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        // Consumer initialization.
        BStruct consumerStruct = (BStruct) context.getRefArgument(0);
        BStruct consumerConfig = (BStruct) consumerStruct.getRefField(0);
        // Check whether consumer configuration is available.
        if (Objects.isNull(consumerConfig)) {
            context.setReturnValues(BLangVMErrors.
                    createError(context,
                     "Kafka consumer is not initialized with consumer configuration."));
        }
        // Check whether already native consumer is attached to the struct.
        // This can be happen either from Kafka service or via programmatically.
        if (Objects.nonNull(consumerStruct.getNativeData(NATIVE_CONSUMER))) {
            context.setReturnValues(BLangVMErrors.createError(context,
                    "Kafka consumer is already connected to external broker." +
                    " Please close it before re-connecting the external broker again."));
        }

        Properties consumerProperties = KafkaUtils.processKafkaConsumerConfig(consumerConfig);

        try {
            KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
            consumerStruct.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
        } catch (KafkaException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}

