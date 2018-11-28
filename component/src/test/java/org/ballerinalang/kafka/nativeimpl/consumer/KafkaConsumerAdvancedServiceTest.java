package org.ballerinalang.kafka.nativeimpl.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.BServiceUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BString;
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

public class KafkaConsumerAdvancedServiceTest {

    private CompileResult compileResult;
    private static File dataDir;
    private static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
        kafkaCluster.createTopic("test", 1, 1);
    }

    @Test(
            description = "Test endpoint bind to a service",
            sequential = true
    )
    public void testKafkaServiceEndpoint() {
        String testString = "test_string";
        //CountDownLatch completion = new CountDownLatch(1);
        compileResult = BCompileUtil.compileAndSetup("consumer/kafka_consumer_advanced_service.bal");
        BServiceUtil.runService(compileResult);
        BRunUtil.invokeStateful(compileResult, "funcKafkaProduce");

        try {
            await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] returnBValues = BRunUtil.invokeStateful(compileResult, "funcKafkaGetResultText");
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BString);
                return (returnBValues[0].stringValue().equals(testString));
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
