/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.net.kafka.nativeimpl;


import org.ballerinalang.annotation.JavaSPIService;
import org.ballerinalang.connector.api.Annotation;
import org.ballerinalang.connector.api.BallerinaConnectorException;
import org.ballerinalang.connector.api.BallerinaServerConnector;
import org.ballerinalang.connector.api.Service;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.net.kafka.api.KafkaListener;
import org.ballerinalang.net.kafka.api.KafkaServerConnector;
import org.ballerinalang.net.kafka.exception.KafkaConnectorException;
import org.ballerinalang.net.kafka.impl.KafkaListenerImpl;
import org.ballerinalang.net.kafka.impl.KafkaServerConnectorImpl;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * {@code KafkaBalServerConnector} This is the Kafka implementation for the {@code BallerinaServerConnector} API.
 */
@JavaSPIService("org.ballerinalang.connector.api.BallerinaServerConnector")
public class KafkaBalServerConnector implements BallerinaServerConnector {

    private Map<String, KafkaServerConnector> connectorMap = new HashMap<>();

    @Override
    public String getProtocolPackage() {
        return Constants.KAFKA_NATIVE_PACKAGE;
    }

    @Override
    public void serviceRegistered(Service service) throws BallerinaConnectorException {
        Annotation kafkaConfig = service.getAnnotation(Constants.KAFKA_NATIVE_PACKAGE,
                Constants.ANNOTATION_KAFKA_CONFIGURATION);
        if (kafkaConfig == null) {
            throw new BallerinaException("Error kafka 'configuration' annotation missing in " + service.getName());
        }

        Properties configParams = KafkaUtils.processKafkaConsumerConfig(kafkaConfig);
        String serviceId = service.getName();

        try {
            KafkaListener kafkaListener = new KafkaListenerImpl(KafkaUtils.extractKafkaResource(service));
            KafkaServerConnector serverConnector = new KafkaServerConnectorImpl(serviceId,
                    configParams, kafkaListener);
            connectorMap.put(serviceId, serverConnector);
            serverConnector.start();
        } catch (KafkaConnectorException e) {
            throw new BallerinaException("Error when starting to listen to the kafka topic while "
                    + serviceId + " deployment", e);
        }
    }

    @Override
    public void serviceUnregistered(Service service) throws BallerinaConnectorException {
        String serviceId = service.getName();
        try {
            KafkaServerConnector serverConnector = connectorMap.get(serviceId);
            if (null != serverConnector) {
                serverConnector.stop();
            }
        } catch (KafkaConnectorException e) {
            throw new BallerinaException("Error while stopping the kafka server connector related with the service "
                    + serviceId, e);
        }
    }

    @Override
    public void deploymentComplete() throws BallerinaConnectorException {

    }
}
