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
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Native function ballerina.net.kafka:connect which connects consumer to remote cluster.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "connect",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "KafkaConsumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "KafkaConsumer",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = {@ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class Connect extends AbstractNativeFunction {
    private static final Logger log = LoggerFactory.getLogger(Connect.class);

    @Override
    public BValue[] execute(Context context) {
        // Consumer initialization
        BStruct consumerStruct = (BStruct) getRefArgument(context, 0);
        BMap<String, BString> consumerBalConfig = (BMap<String, BString>) consumerStruct.getRefField(0);

        Properties consumerProperties = KafkaUtils.processKafkaConsumerConfig(consumerBalConfig);

        try {
            KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
            consumerStruct.addNativeData(Constants.NATIVE_CONSUMER, kafkaConsumer);
        } catch (KafkaException e) {
            return getBValues(BLangVMErrors.createError(context, 0, e.getMessage()));
        }

        return VOID_RETURN;
    }

}

