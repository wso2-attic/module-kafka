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

kafka:ConsumerConfig consumerConfig = {
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    clientId: "basic-consumer",
    offsetReset: "earliest",
    topics: ["test"]
};

function funcKafkaConnect() returns kafka:SimpleConsumer {
    kafka:SimpleConsumer kafkaConsumer = new(consumerConfig);
    return kafkaConsumer;
}

function funcKafkaClose(kafka:SimpleConsumer consumer) returns boolean {
    kafka:SimpleConsumer consumerEP = consumer;
    var conErr = consumerEP->close();
    return true;
}

function funcKafkaGetSubscription(kafka:SimpleConsumer consumer) returns string[]|error {
    kafka:SimpleConsumer consumerEP = consumer;
    string[] topics = [];
    topics = check consumerEP->getSubscription();
    return topics;
}

function funcKafkaGetAssignment(kafka:SimpleConsumer consumer) returns kafka:TopicPartition[]|error {
    kafka:SimpleConsumer consumerEP = consumer;
    kafka:TopicPartition[] partitions = [];
    partitions = check consumerEP->getAssignment();
    return partitions;
}

function funcKafkaPoll(kafka:SimpleConsumer consumer) returns int|error {
    kafka:SimpleConsumer consumerEP = consumer;
    kafka:ConsumerRecord[] records = [];

    records = check consumerEP->poll(1000);
    return records.length();
}
