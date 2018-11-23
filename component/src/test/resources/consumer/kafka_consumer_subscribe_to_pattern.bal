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

kafka:ConsumerConfig consumerConfigs = {
    bootstrapServers: "localhost:9098",
    groupId: "test-group",
    autoCommit: false
};

kafka:SimpleConsumer kafkaConsumer = new (consumerConfigs);

kafka:ProducerConfig producerConfigs = {
    bootstrapServers: "localhost:9098",
    clientID: "basic-producer",
    acks: "all",
    noRetries: 3
};

kafka:SimpleProducer kafkaProducer = new (producerConfigs);

function funcKafkaTestSubscribeToPattern() {
    var result = kafkaConsumer->subscribeToPattern("test.*");
}

function funcKafkaTestGetTopicCount() returns int|error {
    string[] subscribedTopics = check kafkaConsumer->getSubscription();
    return (subscribedTopics.length());
}

function funcKafkaGetAvailableTopicsCount() returns int|error {
    string[] availableTopics = check kafkaConsumer->getAvailableTopics();
    return (availableTopics.length());
}

function funcKafkaProduce() {
    string msg = "Hello World";
    byte[] byteMsg = msg.toByteArray("UTF-8");
    kafkaProducer->send(byteMsg, "test1");
    kafkaProducer->send(byteMsg, "test2");
    kafkaProducer->send(byteMsg, "tester");
    kafkaProducer->send(byteMsg, "another-topic");
}
