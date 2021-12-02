package io.grpc.examples.helloworld

import com.google.protobuf.GeneratedMessageV3
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord


typealias KafkaPayload = ByteArray
typealias KafkaKey = String


object KafkaCommandEventHelper {
    private const val SERVERS = "192.168.193.80:9092"
    private val KEY_SERIAL = StringSerializer::class.java
    private val KEY_DESERIAL = StringDeserializer::class.java
    private val VALUE_SERIAL = ByteArraySerializer::class.java
    private val VALUE_DESERIAL = ByteArrayDeserializer::class.java
    private const val CLIENT_ID_PRODUCER = "command-writer"
    private val log: Logger = LoggerFactory.getLogger(KafkaCommandEventHelper::class.java)

    private val writeProps: Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to SERVERS,
        ProducerConfig.CLIENT_ID_CONFIG to CLIENT_ID_PRODUCER,
        ProducerConfig.ACKS_CONFIG to "1",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to KEY_SERIAL,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to VALUE_SERIAL
    )

    private fun readOptions(consumerName: String, groupName: String): Map<String, Any> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to SERVERS,
            ConsumerConfig.CLIENT_ID_CONFIG to consumerName,
            ConsumerConfig.GROUP_ID_CONFIG to groupName,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KEY_DESERIAL,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to VALUE_DESERIAL
        )

    fun write(topicName: String, kv: Flux<Pair<KafkaKey, GeneratedMessageV3>>) {
        log.info("Start kafka write...")
        val newFlux = kv.map {
            SenderRecord.create(topicName, 0, System.currentTimeMillis(), it.first, it.second.toByteArray(), it.first)
        }
        KafkaSender.create(SenderOptions.create<KafkaKey, KafkaPayload>(writeProps)).send(newFlux)
            .doOnError {
                log.error("Kafka send failed: {}", it)
            }
            .subscribe {
                log.info("Kafka write result correlation = {}", it.correlationMetadata())

            }
        log.info("Completed kafka write")
    }

    fun read(
        consumerName: String, groupName: String, topicName: String,
        readEarliest: Boolean = false
    ): Flux<ReceiverRecord<KafkaKey /* = kotlin.String */, KafkaPayload /* = kotlin.ByteArray */>> =
        with(
            if (readEarliest) mapOf(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest") else mapOf<String, Any>()
        ) {
            KafkaReceiver.create(
                ReceiverOptions.create<KafkaKey, KafkaPayload>(readOptions(consumerName, groupName) + this)
                    .subscription(setOf(topicName))
                    .addAssignListener { log.debug("partitions assigned {}", it) }
                    .addRevokeListener { log.debug("partitions revoked {}", it) }
            ).receive()
        }


}
