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

package org.ballerinalang.net.kafka.nativeimpl.actions.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.net.kafka.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * {@code Init}.
 *
 * @since 0.9
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "<init>",
        connectorName = Constants.CONSUMER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR)
        })
public class Init extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(Init.class);

    @Override
    public ConnectorFuture execute(Context context) {
        // Consumer initialization

        BConnector consumerConnector = (BConnector) getRefArgument(context, 0);
        BStruct consumerConf = ((BStruct) consumerConnector.getRefField(0));
        BMap<String, BString> consumerBalConfig = (BMap<String, BString>) consumerConf.getRefField(0);

        Properties consumerProperties = new Properties();
        for (String key : consumerBalConfig.keySet()) {
            consumerProperties.put(key, consumerBalConfig.get(key).stringValue());
        }
        KafkaConsumer kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
        BStruct consumerStruct = ((BStruct) consumerConnector.getRefField(1));
        consumerStruct.addNativeData(Constants.NATIVE_CONSUMER, kafkaConsumer);
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

    @Override
    public boolean isNonBlockingAction() {
        return false;
    }
}

