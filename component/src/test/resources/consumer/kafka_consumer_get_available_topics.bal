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
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    clientId: "available-topic-consumer",
    offsetReset: "earliest",
    topics: ["test"]
};

kafka:ConsumerConfig consumerConfigsNoTimeout = {
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    defaultApiTimeout: -1
};

function funcKafkaConnect() returns kafka:Listener {
    kafka:Listener kafkaConsumer = new(consumerConfigs);
    return kafkaConsumer;
}

function funcKafkaGetNoTimeoutConsumer() returns kafka:Listener {
    kafka:Listener kafkaConsumer = new(consumerConfigsNoTimeout);
    return kafkaConsumer;
}

function funcKafkaClose(kafka:Listener consumer) returns boolean {
    var result = consumer->close();
    return !(result is error);
}

function funcKafkaGetAvailableTopicsWithDuration(kafka:Listener consumer, int duration) returns string[]|error {
    return consumer->getAvailableTopics(duration = duration);
}

function funcKafkaGetAvailableTopics(kafka:Listener consumer) returns string[]|error {
    return consumer->getAvailableTopics();
}


