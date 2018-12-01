package org.ballerinalang.kafka.nativeimpl.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.kafka.util.KafkaConstants;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.codegen.ProgramFile;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;

public class KafkaConsumerUnsubscribeTest {
    private CompileResult result;
    private static File dataDir;
    private static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
    }

    @Test(
            description = "Test functionality of getAvailableTopics() function",
            sequential = true
    )
    public void testKafkaConsumerUnsubscribe () {
        result = BCompileUtil.compileAndSetup("consumer/kafka_consumer_unsubscribe.bal");
        BValue[] inputBValues = {};
        BValue[] returnBValues = BRunUtil.invokeStateful(result, "funcKafkaTestUnsubscribe", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BBoolean);
        Assert.assertTrue(((BBoolean) returnBValues[0]).booleanValue());
    }

    @AfterClass
    public void tearDown() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
            boolean delete = dataDir.delete();
            // If files are still locked and a test fails: delete on exit to allow subsequent test execution
            if (!delete) {
                dataDir.deleteOnExit();
            }
        }
    }

    private static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-consumer-unsubscribe-test");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9094);
        return kafkaCluster;
    }

    private BMap<String, BValue> createPartitionStruct(ProgramFile programFile) {
        return BLangConnectorSPIUtil.createBStruct(programFile,
                KAFKA_NATIVE_PACKAGE,
                KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
    }
}
