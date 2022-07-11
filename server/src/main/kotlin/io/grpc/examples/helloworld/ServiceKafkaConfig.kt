package io.grpc.examples.helloworld

interface ServiceKafkaConfig {
    val kafkaServers: String
        get() = "localhost:9092"
    val kafkaConsumerGroup: String
        get() = "ComparisonService"
    val kafkaConsumerName: String
        get() = "ComparisonService"
    val numberPartitions: Int
        get() = 10

}