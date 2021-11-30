package io.grpc.examples.helloworld

import com.google.protobuf.util.JsonFormat
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord


class KafkaMessageWriter(private val topicName: String) {

    private val props = mapOf(
        Pair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
        Pair(ProducerConfig.CLIENT_ID_CONFIG, "command-writer"),
        Pair(ProducerConfig.ACKS_CONFIG, "all"),
        Pair(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java),
        Pair(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
    )
    private val sender: KafkaSender<String, String> = KafkaSender.create(SenderOptions.create<String, String>(props))

    fun write(kv: Flux<Pair<String, String>>) =
        sender.send<String> {
            kv
                .map {
                    SenderRecord.create(ProducerRecord(topicName, it.first, it.second), it.first)
                }
                .doOnError { log.error("Kafka send failed", it) }
                .subscribe { log.info("Sent record ${it.correlationMetadata()}")
                }
        }

    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaMessageWriter::class.java)
    }
}