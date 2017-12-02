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

package org.ballerinalang.net.kafka.future;

import org.ballerinalang.connector.api.BallerinaConnectorException;
import org.ballerinalang.connector.api.ConnectorFutureListener;
import org.ballerinalang.model.values.BValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

/**
 * {@code }
 */
public class KafkaPollCycleFutureListener implements ConnectorFutureListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaPollCycleFutureListener.class);
    private Semaphore flowControl;

    /**
     * future will get notified by the kafka native methods and from the Ballerina engine when the Resource invocation
     * is over or when an error occurred.
     */
    public KafkaPollCycleFutureListener(Semaphore loopShutdown) {
        this.flowControl = loopShutdown;
    }

    @Override
    public void notifySuccess() {
        flowControl.release();
    }

    @Override
    public void notifyReply(BValue... response) {
        // not used
    }

    @Override
    public void notifyFailure(BallerinaConnectorException ex) {
        flowControl.release();
    }

}


