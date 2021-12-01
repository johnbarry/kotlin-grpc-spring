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
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord

typealias KafkaPayload = ByteArray
typealias KafkaKey = String

abstract class KafkaConfig {
    val SERVERS = "localhost:9092"
    val KEY_SERIAL =  StringSerializer::class.java
    val KEY_DESERIAL =  StringDeserializer::class.java
    val VALUE_SERIAL = ByteArraySerializer::class.java
    val VALUE_DESERIAL = ByteArrayDeserializer::class.java
    val CLIENT_ID_PRODUCER = "command-writer"
    val CLIENT_ID_CONSUMER = "command-reader"
    val CLIENT_ID_GROUP = "command-reader-group"
}


class KafkaMessageWriter(private val topicName: String) : KafkaConfig() {

    private val props: Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to SERVERS,
        ProducerConfig.CLIENT_ID_CONFIG to CLIENT_ID_PRODUCER,
        ProducerConfig.ACKS_CONFIG to "1",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to  KEY_SERIAL,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to VALUE_SERIAL
    )
    private val sender: KafkaSender<KafkaKey, KafkaPayload> = KafkaSender.create(SenderOptions.create(props))

    fun write(kv: Flux<Pair<KafkaKey, GeneratedMessageV3>>) {
        log.info("Start kafka write...")
        val newFlux = kv.map {
            val payload = it.second.toByteArray()
            SenderRecord.create(topicName,0, System.currentTimeMillis(), it.first, payload, it.first)
        }
        sender.send (newFlux)
            .doOnError {
                log.error("Kafka send failed: {}", it)
            }
            .subscribe {
                log.info("Kafka write result correlation = {}", it.correlationMetadata())

            }
        log.info("Completed kafka write")
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaMessageWriter::class.java)
    }
}

class KafkaMessageReader(private val topicName: String, val readEarliest: Boolean = false) : KafkaConfig()  {

    private val props: Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to SERVERS,
        ConsumerConfig.CLIENT_ID_CONFIG to CLIENT_ID_CONSUMER,
        ConsumerConfig.GROUP_ID_CONFIG to CLIENT_ID_GROUP,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KEY_DESERIAL,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to VALUE_DESERIAL
    ) + (if (readEarliest) mapOf(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest") else mapOf<String,Any>())

    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaMessageWriter::class.java)
    }
}