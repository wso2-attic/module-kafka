package org.ballerinalang.kafka.nativeimpl.producer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BLangRuntimeException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerCloseTest {
    private CompileResult result;
    private static File dataDir;
    protected static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        result = BCompileUtil.compile("producer/kafka_producer_close.bal");
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
        kafkaCluster.createTopic("test", 2, 1);
    }

    @Test(
            description = "Test Producer close() action",
            expectedExceptions = BLangRuntimeException.class,
            expectedExceptionsMessageRegExp = ".*Cannot perform operation after producer has been closed.*"
    )
    public void testKafkaProduce() {
        BValue[] inputBValues = {};
        BRunUtil.invoke(result, "funcTestKafkaClose", inputBValues);

        final CountDownLatch completion = new CountDownLatch(1);
        final AtomicLong messagesRead = new AtomicLong(0);
        kafkaCluster.useTo().consumeStrings("test", 2, 10, TimeUnit.SECONDS, completion::countDown, (key, value) -> {
            messagesRead.incrementAndGet();
            return true;
        });

        try {
            completion.await();
        } catch (Exception ex) {
            //Ignore
        }
        Assert.assertEquals(messagesRead.get(), 1);
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

    protected static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-producer");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2182, 9094);
        return kafkaCluster;
    }
}
