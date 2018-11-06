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
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.ballerinalang.util.transactions.LocalTransactionInfo;

import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER_CONFIG;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;

/**
 * Native action aborts an ongoing transaction for the provided producer.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "abortTransaction",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class AbortTransaction extends AbstractTransactionHandler {

    @Override
    public void execute(Context context, CallableUnitCallback callback) {

        this.context = context;
        BMap<String, BValue> producerConnector = (BMap<String, BValue>) context.getRefArgument(0);

        BMap producerMap = (BMap) producerConnector.get("producerHolder");
        BMap<String, BValue> producerStruct = (BMap<String, BValue>) producerMap.get(new BString(NATIVE_PRODUCER));

        String connectorKey = producerConnector.get("connectorID").stringValue();
        this.producer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);
        Properties producerProperties = (Properties) producerStruct.getNativeData(NATIVE_PRODUCER_CONFIG);

        LocalTransactionInfo localTransactionInfo = context.getLocalTransactionInfo();

        try {
            if (isTransactionalProducer(producerProperties) &&
                    isKafkaTransactionInitiated(localTransactionInfo, connectorKey)) {
                producer.abortTransaction();
            }
        } catch (KafkaException e) {
            throw new BallerinaException("Failed to abort the transaction. " + e.getMessage(), e, context);
        }
    }
}
