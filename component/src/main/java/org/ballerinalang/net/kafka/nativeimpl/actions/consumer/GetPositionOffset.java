/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.net.kafka.nativeimpl.actions.consumer;

import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code }
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "getPositionOffset",
        connectorName = Constants.CONSUMER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "partition", type = TypeKind.STRUCT, structType = "TopicPartition",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = { @ReturnType(type = TypeKind.INT), @ReturnType(type = TypeKind.STRUCT)})
public class GetPositionOffset extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(GetPositionOffset.class);

    @Override
    public ConnectorFuture execute(Context context) {

        //  Extract argument values
        //  BConnector bConnector = (BConnector) getRefArgument(context, 0);
        //  BStruct messageStruct = ((BStruct) getRefArgument(context, 1));
        //  String destination = getStringArgument(context, 0);
        


        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}

