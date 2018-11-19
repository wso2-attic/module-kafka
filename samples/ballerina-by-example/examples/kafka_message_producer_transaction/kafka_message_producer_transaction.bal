// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import wso2/kafka;
import ballerina/io;

endpoint kafka:SimpleProducer kafkaProducer {
    // Here we create a producer configs with optional parameters client.id - used for broker side logging.
    // acks - number of acknowledgments for request complete,
    // noRetries - number of retries if record send fails.
    bootstrapServers:"localhost:9092",
    clientID:"basic-producer",
    acks:"all",
    noRetries:3,
    transactionalID:"test-transactional-id"
};

public function main(string... args) {
    string msg = "Hello World Transaction";
    byte[] serializedMsg = msg.toByteArray("UTF-8");

    // Here we create a producer configs with optional parameter transactional.id - enable transactional message production.
    kafkaAdvancedTransactionalProduce(serializedMsg);
}

function kafkaAdvancedTransactionalProduce(byte[] msg) {
    // Kafka transactions allows messages to be send multiple partition atomically on KafkaProducerClient. Kafka Local transactions can only be used
    // when you are sending multiple messages using the same KafkaProducerClient instance.
    boolean transactionSuccess = false;
    transaction {
        kafkaProducer->send(msg, "test-kafka-topic", partition = 0);
        kafkaProducer->send(msg, "test-kafka-topic", partition = 0);
        transactionSuccess = true;
    }

    if (transactionSuccess) {
        io:println("Transaction committed");
    } else {
        io:println("Transaction failed");
    }
}
