package org.ballerinalang.kafka.nativeimpl.consumer.endpoint;

import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.BlockingNativeCallableUnit;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.Struct;
import org.ballerinalang.kafka.exception.KafkaConnectorException;
import org.ballerinalang.kafka.impl.KafkaServerConnectorImpl;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_ENDPOINT_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_SERVER_CONNECTOR_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;

/**
 * Start server connector.
 */

@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "start",
        receiver = @Receiver(
                type = TypeKind.OBJECT,
                structType = CONSUMER_ENDPOINT_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE
        ),
        isPublic = true
)
public class Start extends BlockingNativeCallableUnit {

    @Override
    public void execute(Context context) {
        Struct service = BLangConnectorSPIUtil.getConnectorEndpointStruct(context);
        KafkaServerConnectorImpl serverConnector = (KafkaServerConnectorImpl) service
                .getNativeData(CONSUMER_SERVER_CONNECTOR_NAME);
        try {
            serverConnector.start();
        } catch (KafkaConnectorException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
            return;
        }
        context.setReturnValues();
    }
}
