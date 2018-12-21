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
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import static org.ballerinalang.kafka.util.KafkaConstants.FULL_PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native action producer records from record accumulator.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = FULL_PACKAGE_NAME,
        functionName = "flushRecords",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE)
)
public class FlushRecords implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BMap<String, BValue> producerConnector = (BMap<String, BValue>) context.getRefArgument(0);
        BMap producerMap = (BMap) producerConnector.get("producerHolder");
        BMap<String, BValue> producerStruct = (BMap<String, BValue>) producerMap.get(new BString(NATIVE_PRODUCER));
        KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);
        try {
            kafkaProducer.flush();
        } catch (KafkaException e) {
            context.setReturnValues(createError(context, "Failed to flush the records. " + e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
