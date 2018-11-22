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

endpoint kafka:SimpleConsumer kafkaConsumer {
    bootstrapServers: "localhost:9098",
    groupId: "test-group",
    autoCommit: false
};

endpoint kafka:SimpleProducer kafkaProducer {
    bootstrapServers: "localhost:9098",
    clientID: "basic-producer",
    acks: "all",
    noRetries: 3
};

function funcKafkaTestSubscribeToPattern() {
    var result = kafkaConsumer->subscribeToPattern("test.*");
}

function funcKafkaTestGetTopicCount() returns int {
    string[] subscribedTopics = check kafkaConsumer->getSubscription();
    return (lengthof subscribedTopics);
}

function funcKafkaGetAvailableTopicsCount() returns int {
    string[] availableTopics = check kafkaConsumer->getAvailableTopics();
    return (lengthof availableTopics);
}

function funcKafkaProduce() {
    string msg = "Hello World";
    byte[] byteMsg = msg.toByteArray("UTF-8");
    kafkaProducer->send(byteMsg, "test1");
    kafkaProducer->send(byteMsg, "test2");
    kafkaProducer->send(byteMsg, "tester");
    kafkaProducer->send(byteMsg, "another-topic");
}
