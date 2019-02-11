/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.kafka.nativeimpl.consumer.service;

import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.Struct;
import org.ballerinalang.kafka.exception.KafkaConnectorException;
import org.ballerinalang.kafka.impl.KafkaServerConnectorImpl;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_SERVER_CONNECTOR_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.FULL_PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;

/**
 * Start server connector.
 */

@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = FULL_PACKAGE_NAME,
        functionName = "stop",
        receiver = @Receiver(
                type = TypeKind.OBJECT,
                structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE
        ),
        isPublic = true
)
public class Stop implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        Struct service = BLangConnectorSPIUtil.getConnectorEndpointStruct(context);
        KafkaServerConnectorImpl serverConnector = (KafkaServerConnectorImpl) service
                .getNativeData(CONSUMER_SERVER_CONNECTOR_NAME);
        try {
            serverConnector.stop();
        } catch (KafkaConnectorException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
            return;
        }
        context.setReturnValues();
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
