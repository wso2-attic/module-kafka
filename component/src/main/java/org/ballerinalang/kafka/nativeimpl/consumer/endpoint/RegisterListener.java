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

package org.ballerinalang.kafka.nativeimpl.consumer.endpoint;

import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.Service;
import org.ballerinalang.kafka.api.KafkaListener;
import org.ballerinalang.kafka.api.KafkaServerConnector;
import org.ballerinalang.kafka.exception.KafkaConnectorException;
import org.ballerinalang.kafka.impl.KafkaListenerImpl;
import org.ballerinalang.kafka.impl.KafkaServerConnectorImpl;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_ENDPOINT_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;

/**
 * This is used to register a listener to the kafka service.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "registerListener",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = CONSUMER_ENDPOINT_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = {@Argument(name = "serviceType", type = TypeKind.TYPEDESC)}
)
public class RegisterListener implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        Service service = BLangConnectorSPIUtil.getServiceRegistered(context);
        BStruct consumerEndpoint = (BStruct) context.getRefArgument(0);
        BStruct consumerConfig = (BStruct) consumerEndpoint.getRefField(1);

        Properties configParams = KafkaUtils.processKafkaConsumerConfig(consumerConfig);
        String serviceId = service.getName();

        try {
            KafkaListener kafkaListener = new KafkaListenerImpl(KafkaUtils.extractKafkaResource(service));
            KafkaServerConnector serverConnector = new KafkaServerConnectorImpl(serviceId, configParams, kafkaListener);
            serverConnector.start();
        } catch (KafkaConnectorException e) {
            context.setReturnValues(BLangVMErrors.createError(context, 0, e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
