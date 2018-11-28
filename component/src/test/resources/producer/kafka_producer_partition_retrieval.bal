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

kafka:ProducerConfig producerConfigs = {
    bootstrapServers: "localhost:9094",
    clientID: "partition-retrieval-producer",
    acks: "all",
    noRetries: 3
};

kafka:SimpleProducer kafkaProducer = new(producerConfigs);

function funcTestPartitionInfoRetrieval(string topic) returns kafka:TopicPartition[] {
    return getPartitionInfo(topic);
}

function getPartitionInfo(string topic) returns kafka:TopicPartition[] {
    kafka:TopicPartition[] partitions = kafkaProducer->getTopicPartitions(topic);
    var result = kafkaProducer->close();
    return partitions;
}
