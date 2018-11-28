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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import wso2/kafka;
import ballerina/io;

string topic = "test";

kafka:ConsumerConfig consumerConfigs = {
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    clientId: "advanced-service-consumer",
    offsetReset: "earliest",
    topics: [topic],
    autoCommit:false
};

listener kafka:SimpleConsumer kafkaConsumer = new (consumerConfigs);

kafka:ProducerConfig producerConfigs = {
    bootstrapServers: "localhost:9094",
    clientID: "advanced-service-producer",
    acks: "all",
    noRetries: 3
};

kafka:SimpleProducer kafkaProducer = new (producerConfigs);

string resultText = "";
int noOfChars = 11;

service kafkaService on kafkaConsumer {
    resource function onMessage(
        kafka:SimpleConsumer consumer,
        kafka:ConsumerRecord[] records,
        kafka:PartitionOffset[] offsets,
        string groupId
    ) {
        foreach kafkaRecord in records {
            byte[] serializedMsg = kafkaRecord.value;
            io:ReadableByteChannel byteChannel = io:createReadableChannel(serializedMsg);
            io:ReadableCharacterChannel characterChannel = new io:ReadableCharacterChannel(byteChannel, "UTF-8");
            var result = characterChannel.read(noOfChars);
            if (result is error) {
                resultText = "Failed";
            } else {
                resultText = untaint result;
            }
        }
    }
}

function funcKafkaGetResultText() returns string {
    return resultText;
}

function funcKafkaProduce() {
    string msg = "test_string";
    byte[] byteMsg = msg.toByteArray("UTF-8");
    var result = kafkaProducer->send(byteMsg, topic);
}
