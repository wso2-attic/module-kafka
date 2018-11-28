package org.ballerinalang.kafka.nativeimpl.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class KafkaConsumerSubscribeToPatternTest {
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
    public void testKafkaConsumerSubscribeToPattern () {
        result = BCompileUtil.compileAndSetup("consumer/kafka_consumer_subscribe_to_pattern.bal");
        BValue[] inputBValues = {};
        try {
            await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] returnBValues = BRunUtil
                        .invokeStateful(result, "funcKafkaTestGetTopicCount", inputBValues);
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BInteger);
                long topicCount = ((BInteger) returnBValues[0]).intValue();
                return (topicCount == 0);
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }

        BRunUtil.invokeStateful(result, "funcKafkaProduce");
        BRunUtil.invokeStateful(result, "funcKafkaTestSubscribeToPattern");

        try {
            await().atMost(20000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] returnBValues = BRunUtil.
                        invokeStateful(result, "funcKafkaGetAvailableTopicsCount", inputBValues);
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BInteger);
                long topicCount = ((BInteger) returnBValues[0]).intValue();
                Assert.assertEquals(topicCount, 4);

                returnBValues = BRunUtil
                        .invokeStateful(result, "funcKafkaTestGetTopicCount", inputBValues);
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BInteger);
                topicCount = ((BInteger) returnBValues[0]).intValue();
                return (topicCount == 3);
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
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
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-consumer");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2185, 9094);
        return kafkaCluster;
    }
}
