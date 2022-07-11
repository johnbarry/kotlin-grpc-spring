package io.grpc.examples.helloworld

import org.apache.kafka.clients.admin.NewTopic

class KafkaService : KafkaServiceGrpcKt.KafkaServiceCoroutineImplBase(), ServiceKafkaConfig {
    override suspend fun kafkaRecordCount(request: KafkaCountRequest): KafkaCountResponse =
        kafkaCountResponse {
            records =
                KafkaHelper(kafkaServers)
                    .let {
                        if (request.hasAllPartitions())
                            it.read("KafkaCount", "KafkaCount", request.topic, readEarliest = true)
                        else
                            it.readPartition(kafkaConsumerGroup, request.topic, request.partition, readEarliest = true)
                    }
                    .count()
                    .block() ?: 0L

        }
    override suspend fun createTopic(request: TopicCreationRequest): Empty {
        admin {
            servers = kafkaServers
            assert(request.topic != null)
            client?.createTopics(
                listOf(
                    request.topic
                ).map {
                    NewTopic(it!!,
                        if (request.partitions==0) 10 else request.partitions,
                        (if (request.replicaCount == 0) 1 else request.replicaCount).toShort()
                    )
                }
            )
                ?.all()
                ?.get()
        }
        return Empty.getDefaultInstance()
    }

    override suspend fun deleteTopic(request: TopicDeletionRequest): Empty {
        admin {
            servers = kafkaServers
            assert(request.topic != null)
            client?.deleteTopics(
                listOf(
                    request.topic
                )
            )
                ?.all()
                ?.get()
        }
        return Empty.getDefaultInstance()
    }


}
