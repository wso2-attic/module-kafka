// Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import ballerina/encoding;

string topic = "test-topic-ssl";

kafka:ProducerConfig producerConfigs = {
    bootstrapServers: "localhost:9094",
    clientId:"ssl-producer",
    acks:"all",
    noRetries:3,
    secureSocket: {
        keyStore:{
            location:"<FILE_PATH>/kafka.client.keystore.jks",
            password:"test1234"
        },
        trustStore: {
            location:"<FILE_PATH>/kafka.client.truststore.jks",
            password:"test1234"
        },
        protocol: {
            sslProtocol:"TLS",
            sslProtocolVersions:"TLSv1.2,TLSv1.1,TLSv1",
            securityProtocol:"SSL"
        },
        sslKeyPassword:"test1234"
    }
};

kafka:ConsumerConfig consumerConfig = {
    bootstrapServers:"localhost:9094",
    groupId:"test-group",
    clientId: "ssl-consumer",
    offsetReset:"earliest",
    topics:["test-topic-ssl"],
    secureSocket: {
        keyStore:{
            location:"<FILE_PATH>/kafka.client.keystore.jks",
            password:"test1234"
        },
        trustStore: {
            location:"<FILE_PATH>/kafka.client.truststore.jks",
            password:"test1234"
        },
        protocol: {
            sslProtocol:"TLS",
            sslProtocolVersions:"TLSv1.2,TLSv1.1,TLSv1",
            securityProtocol:"SSL"
        },
        sslKeyPassword:"test1234"
    }
};

kafka:ProducerConfig producerNegativeConfigs = {
    bootstrapServers: "localhost:9094",
    clientId:"ssl-producer-negative",
    acks:"all",
    noRetries:3
};

kafka:Producer kafkaProducer = new(producerConfigs);
kafka:Consumer consumer = new(consumerConfig);
kafka:Producer negativeProducer = new (producerNegativeConfigs);

function funcTestKafkaProduceWithSSL(string msg) returns boolean|error {
    byte[] byteMsg = msg.toByteArray("UTF-8");
    var result = kafkaProducer->send(byteMsg, topic);
    if (result is error) {
        return result;
    } else {
        return true;
    }
}

function funcKafkaPollWithSSL() returns string|error {
    var results = consumer->poll(1000);
    if (results is error) {
        return results;
    } else if (results.length() == 1) {
        var kafkaRecord = results[0];
        byte[] serializedMsg = kafkaRecord.value;
        return encoding:byteArrayToString(serializedMsg);
    } else {
        return "";
    }
}

function funcKafkaSSLConnectNegative() returns int|error {
    string msg = "Hello World SSL Negative Test";
    byte[] byteMsg = msg.toByteArray("UTF-8");
    var result = negativeProducer->send(byteMsg, topic);
    if (result is error) {
        return result;
    } else {
        return 1;
    }
}
