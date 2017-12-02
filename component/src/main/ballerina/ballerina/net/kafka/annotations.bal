package ballerina.net.kafka;

public annotation configuration attach service<> {
    string bootstrapServers;
    string groupId;
    string[] topics;
    int pollingTimeout;
    int concurrentConsumers;
    boolean autoCommit;
    boolean decoupleProcessing;
    string[] properties;
}
