package io.grpc.examples.helloworld

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord


class KafkaMessageWriter(private val topicName: String) {

    private val props: Map<String, Any> = mapOf(
        Pair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
        Pair(ProducerConfig.CLIENT_ID_CONFIG, "command-writer"),
        Pair(ProducerConfig.ACKS_CONFIG, "1"),
        Pair(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java),
        Pair(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
    )
    private val sender: KafkaSender<String, String> = KafkaSender.create(SenderOptions.create(props))

    fun write(kv: Flux<Pair<String, String>>) {
        log.info("Start kafka write...")
        val newFlux = kv.map {
            SenderRecord.create(topicName,0, System.currentTimeMillis(), it.first, it.second, it.first)
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