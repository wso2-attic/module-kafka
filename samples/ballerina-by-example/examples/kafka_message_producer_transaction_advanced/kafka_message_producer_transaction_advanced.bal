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
import ballerina/internal;

endpoint kafka:SimpleProducer kafkaProducer {
    // Here we create a producer configs with optional parameters client.id - used for broker side logging.
    // acks - number of acknowledgments for request complete,
    // noRetries - number of retries if record send fails.
    bootstrapServers:"localhost:9092",
    clientID:"basic-producer",
    acks:"all",
    noRetries:5,
    transactionalID:"test-transactional-id"
};

endpoint kafka:SimpleConsumer consumer {
    bootstrapServers:"localhost:9092",
    groupId:"group-id",
    topics:["test-kafka-topic"],
    pollingInterval:1000
};

service<kafka:Consumer> kafkaService bind consumer {

    onMessage(kafka:ConsumerAction consumerAction, kafka:ConsumerRecord[] records) {
        // Dispatched set of Kafka records to service, We process each one by one.
        foreach kafkaRecord in records {
            processKafkaRecord(kafkaRecord);
        }
        string msg = "Hello World Advanced Transaction";
        byte[] serializedMsg = msg.toByteArray("UTF-8");

        kafkaTransactionalCTP(serializedMsg, consumerAction);
        // Please note we have omitted calling consumer.commit() ( enable.auto.commit = false ) now this is handled inside the
        // transaction block as these offsets are committed part of transaction.
    }
}

function kafkaTransactionalCTP(byte[] msg, kafka:ConsumerAction consumer) {
    // Here we do several produces and consumer commit atomically.
    transaction with oncommit = onCommitFunction, onabort = onAbortFunction {
        kafkaProducer->send(msg, "test-kafka-topic", partition = 0);
        kafkaProducer->send(msg, "test-kafka-topic", partition = 0);
        kafkaProducer->commitConsumer(consumer);
    }
}

function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) {
    byte[] serializedMsg = kafkaRecord.value;
    string msg = internal:byteArrayToString(serializedMsg, "UTF-8");
    // Print the retrieved Kafka record.
    io:println("Topic: " + kafkaRecord.topic + " Received Message: " + msg);
}

function onCommitFunction(string transactionId) {
    io:println("Transaction: " + transactionId + " committed");
}

function onAbortFunction(string transactionId) {
    io:println("Transaction: " + transactionId + " aborted");
}