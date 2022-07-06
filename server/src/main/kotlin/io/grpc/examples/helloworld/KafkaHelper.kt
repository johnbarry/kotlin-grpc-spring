package io.grpc.examples.helloworld

import com.google.protobuf.GeneratedMessageV3
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.admin.AdminClientConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.time.Duration
import java.util.UUID

typealias KafkaPayload = ByteArray
typealias KafkaKey = String

class KafkaHelper(private val servers: String) {
    private val KEY_SERIAL = StringSerializer::class.java
    private val KEY_DES = StringDeserializer::class.java
    private val VALUE_SERIAL = ByteArraySerializer::class.java
    private val VALUE_DES = ByteArrayDeserializer::class.java
    private val CLIENT_ID_PRODUCER = "command-writer"
    private val log: Logger = LoggerFactory.getLogger(KafkaHelper::class.java)

    private val writeProps: Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
        ProducerConfig.CLIENT_ID_CONFIG to CLIENT_ID_PRODUCER,
        ProducerConfig.ACKS_CONFIG to "1",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to KEY_SERIAL,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to VALUE_SERIAL
    )

    private fun readOptions(consumerName: String, groupName: String): Map<String, Any> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
            ConsumerConfig.CLIENT_ID_CONFIG to consumerName,
            ConsumerConfig.GROUP_ID_CONFIG to groupName,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KEY_DES,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to VALUE_DES
        )

    private fun generateUUID(): String = UUID.randomUUID().toString()

    fun writeSinglePartition(topicName: String, kv: Flux<Pair<KafkaKey?, GeneratedMessageV3>>) =
        blockWrite(topicName,
            kv
                .map {
                    Triple(it.first, it.second, 0)
                })

    fun blockWrite(topicName: String, kvPlusPartition: Flux<Triple<KafkaKey?, GeneratedMessageV3, Int>>) {
        log.info("Start kafka write...")
        write(topicName, kvPlusPartition)
            .blockLast()
        log.info("Completed kafka write")
    }
    fun write(topicName: String, kvPlusPartition: Flux<Triple<KafkaKey?, GeneratedMessageV3, Int>>) =
        KafkaSender.create(SenderOptions.create<KafkaKey, KafkaPayload>(writeProps)).send(
            kvPlusPartition.map {
                SenderRecord.create(
                    topicName,
                    it.third,
                    System.currentTimeMillis(),
                    it.first ?: generateUUID(),
                    it.second.toByteArray(),
                    it.first
                )
            }
        )
            .doOnError {
                log.error("Kafka send failed: {}", it)
            }
            .doOnComplete { log.info("$topicName WRITE COMPLETE") }


    fun read(
        consumerName: String, groupName: String, topicName: String,
        readEarliest: Boolean = false
    ): Flux<ConsumerRecord<KafkaKey /* = kotlin.String */, KafkaPayload /* = kotlin.ByteArray */>> =
        KafkaReceiver.create(
            ReceiverOptions.create<KafkaKey, KafkaPayload>(readOptions(consumerName, groupName))
                .subscription(setOf(topicName))
                .addAssignListener { parts ->
                    if (readEarliest)
                        parts.forEach { p -> p.seekToBeginning() }
                }
                .addRevokeListener { log.debug("partitions revoked {}", it) }
        )
            .receiveAutoAck()
            .timeout(Duration.ofSeconds(1L), Mono.empty())
            .flatMap { it }
            //.log()
            .doOnComplete { log.info("$topicName READ COMPLETE ($consumerName)") }
}

class KafkaAdminHelper {
    var client: Admin? = null
    var servers: String? = null
        set(value) {
            client = Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to value))
            field = value
        }
}

fun admin(init: KafkaAdminHelper.() -> Unit) {
    with(KafkaAdminHelper()) {
        init()
    }
}
