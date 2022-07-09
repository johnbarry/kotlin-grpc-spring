/*
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.helloworld

import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import java.time.Instant
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

const val NEW_FRIEND_CMD_TOPIC = "NewFriendCommand"
const val NEW_FRIEND_EVENT_TOPIC = "NewFriendEvent"
const val PERSON_UPDATES_DATA_TOPIC = "PersonDataUpdates"

@SpringBootApplication
@EnableAutoConfiguration
@Component
open class HelloWorldServer : ApplicationRunner {
    private val port = 50051
    private val server: Server = ServerBuilder
        .forPort(port)
        .addService(HelloWorldService())
        .addService(FriendService())
        .build()

    private fun start() {
        server.start()
        log.info("gRPC server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                log.info("*** shutting down gRPC server since JVM is shutting down")
                this@HelloWorldServer.stop()
                log.info("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    private fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class HelloWorldService : GreeterGrpcKt.GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply = helloReply {
            message = "Hello ${request.name}"
            log.info("Client sent sayHello - responding with $message")
        }

        override fun backPressureDemo(request: BackPressureDemoRequest): Flow<ANumber> =
            (if (request.addFiller) 100 else 10_000).let { reportEvery ->
                Flux.range(1, request.number)
                    .map {
                        aNumber {
                            number = it
                            if (request.addFiller)
                                (1..it).forEach { _ -> filler.add("BlahBlahBlah") }
                        }
                    }
                    .doOnNext {
                        val n = it.number
                        if (n % reportEvery == 0)
                            log.info("Server backpressure demo: $n")
                    }
                    .asFlow()
            }
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(HelloWorldServer::class.java)
    }

    override fun run(args: ApplicationArguments?) {
        log.info("Starting grpc Server")
        val server = HelloWorldServer()
        server.start()
        server.blockUntilShutdown()
    }

}

class FriendService : FriendServiceGrpcKt.FriendServiceCoroutineImplBase() {

    private val kafkaServers = "localhost:9092"

    data class ParseException(val key: String, val offset: Long) : Exception() {
        override fun toString(): String = "parse error for key: $key, offset=$offset"
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override suspend fun requestFriend(request: FriendRequest): NewFriendCommand =
        newFriendCommand {
            firstPerson = request.firstPerson
            secondPerson = request.secondPerson
        }.apply {
            KafkaHelper(kafkaServers).writeSinglePartition(NEW_FRIEND_CMD_TOPIC, Flux.just(Pair(null, this)))
        }

    private fun timeNow(): Timestamp = timestamp { seconds = Instant.now().epochSecond }

    @Suppress("UNUSED_PARAMETER")
    fun friendCommandsFlux(
        request: EventStreamRequest,
        consumer: String,
        group: String,
        readEarliest: Boolean
    ): Flux<NewFriendCommand> =
        KafkaHelper(kafkaServers).read(consumer, group, NEW_FRIEND_CMD_TOPIC, readEarliest)
            .map {
                try {
                    NewFriendCommand.parseFrom(it.value())
                        .toBuilder()
                        .setKafkaOffset(it.offset())
                        .build()
                        .apply {
                            HelloWorldServer.log.debug("friend command created: offset ${it.offset()}: key ${it.key()}")
                        }
                } catch (ex: Exception) {
                    throw ParseException(it.key(), it.offset())
                }
            }
            .onErrorContinue { t: Throwable, _: Any ->
                HelloWorldServer.log.error(t.toString())
            }

    override fun friendCommands(request: EventStreamRequest): Flow<NewFriendCommand> =
        friendCommandsFlux(request, "friendCommands-list", "friendCommands-list-grp", true).asFlow()

    override suspend fun makeFriend(request: NewFriendCommand): NewFriendshipEvent = cmdMakeFriend(request)

    private fun cmdMakeFriend(request: NewFriendCommand): NewFriendshipEvent =
        newFriendshipEvent {
            firstPerson = request.firstPerson
            secondPerson = request.secondPerson
            dated = timeNow()
        }.apply {
            KafkaHelper(kafkaServers).writeSinglePartition(NEW_FRIEND_EVENT_TOPIC, Flux.just(Pair(null, this)))
        }

    override fun makeOutstandingFriends(request: EventStreamRequest): Flow<NewFriendshipEvent> =
        friendCommandsFlux(request, "friendCommands-exec", "friendCommands-exec-grp", true)
            .map { cmdMakeFriend(it) }
            .asFlow()

    override suspend fun generateTestData(request: Empty): Empty {
        KafkaHelper(kafkaServers).writeSinglePartition(PERSON_UPDATES_DATA_TOPIC,
            (PersonMock.originalRecords + PersonMock.updatedRecords)
                .asFlow()
                .asFlux()
                .log()
                .map { p -> Pair(p.changeId.toString(), p) }
        )
        return Empty.getDefaultInstance()
    }

    private fun peopleUpdateFlux() =
        KafkaHelper(kafkaServers)
            .read("person-list", "person-list-group", PERSON_UPDATES_DATA_TOPIC, true)
            .map { PersonChange.parseFrom(it.value()) }

    override fun listPeopleChanges(request: Empty): Flow<PersonChange> =
        peopleUpdateFlux().asFlow()

    override fun listPeople(request: Empty): Flow<Person> =
        PersonMock.testNames.asFlow()

    fun listPeople(): Flow<Person> = listPeople(Empty.getDefaultInstance())

    override suspend fun getPerson(request: PersonId): Person =
        listPeople(Empty.getDefaultInstance())
            .filter { p -> p.id == request.id }
            .firstOrNull() ?: Person.getDefaultInstance()

    override suspend fun changeCallback(request: PersonChangeEvent): PersonChange =
        peopleUpdateFlux()
            .filter { it.changeId == request.changeId }
            .blockFirst() ?: PersonChange.getDefaultInstance()

    override fun peopleChangeEvents(request: Empty): Flow<PersonChangeEvent> =
        peopleUpdateFlux()
            .map { p ->
                personChangeEvent {
                    changeId = p.changeId
                    id = p.person.id
                    operation = p.operation
                }
            }
            .asFlow()

}

fun List<String>.multiLineAddress(): String =
    filter { it.isNotEmpty() }
        .joinToString("\n")

class ComparisonService : ComparisonServiceGrpcKt.ComparisonServiceCoroutineImplBase() {

    private val kafkaServers = "localhost:9092"
    private val numberPartitions = 10
    private val topicReplication: Short = 1
    private val kafkaConsumerGroup = "ComparisonService"


    override suspend fun comparePerson(request: PersonComparison): ComparisonResult =
        comparison(request.identifier) {
            compareValue("name") {
                actual = request.actual.name
                expected = request.expected.forename + " " + request.expected.surname
            }
            compareValue("id") {
                actual = request.actual.id.toString()
                expected = request.expected.id.toString()
            }
            comparing("address") {
                if (request.actual.addressCount != 0
                    || request.expected.addressLine1 == null
                    || request.expected.addressLine2 == null
                    || request.expected.city == null
                ) {
                    val expectedAddress = with(request.expected) {
                        listOf(addressLine1, addressLine2, city)
                            .multiLineAddress()
                    }
                    val actualAddress = request.actual.addressList.multiLineAddress()
                    val addressWithoutCity: List<String>? = request.actual.addressList
                        ?.withIndex()
                        ?.filterNot { (_, value) ->
                            value.equals(request.expected.city, true)
                        }
                        ?.map { it.value }

                    if ((addressWithoutCity?.size ?: 0) == request.actual.addressList.size)
                        unexpectedBreak(comparisonBreak {
                            fieldName = this@comparing.field
                            actualValue = actualAddress
                            expectedValue = expectedAddress
                            explain = "Did not find city in address"
                        }) else {
                        if (addressWithoutCity != null) {
                            if (!addressWithoutCity
                                    .multiLineAddress()
                                    .equals(
                                        listOf(request.expected.addressLine1, request.expected.addressLine2)
                                            .multiLineAddress(),
                                        true
                                    )
                            )
                                unexpectedBreak(comparisonBreak {
                                    fieldName = this@comparing.field
                                    actualValue = actualAddress
                                    expectedValue = expectedAddress
                                    explain = "Address line mismatch"
                                })
                        }
                    }
                }
            }
        }

    override suspend fun createTopic(request: TopicCreationRequest): Empty {
        admin {
            servers = kafkaServers
            assert(request.topic != null)
            client?.createTopics(
                listOf(
                    request.topic
                ).map {
                    NewTopic(it!!, numberPartitions, topicReplication)
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


    override suspend fun generateTestData(request: TestDataRequest): TestDataResponse {
        var breaks = 0L
        val pairFlux: Flux<Triple<Person, Person2, Int>> = Flux.fromIterable(1..request.records)
            .map {
                val id = 1L + it
                val partition = id.toInt() % numberPartitions
                with(PersonFaker.fakePersonPair(id)) {
                    if (Random.nextInt(100) <= request.percentBreaks)
                        Triple(
                            first,
                            second.toBuilder()
                                .addAddress("extra address line for break ${++breaks}")
                                .setName(
                                    if (Random.nextInt(100) >= 50)
                                        second.name + " XX"
                                    else second.name
                                )
                                .build(),
                            partition
                        )
                    else
                        Triple(first, second, partition)
                }
            }
            .publish()
            .refCount(2)
        val helper = KafkaHelper(kafkaServers)
        var expectedCount = 0L
        var actualCount = 0L

        val latch = CountDownLatch(2)


        coroutineScope {
            launch {
                helper.writeProto(request.expectedDataTopic,
                    pairFlux
                        .map {
                            Triple(it.first.id.toString(), it.first, it.third)
                        })
                    .doOnComplete {
                        latch.countDown()
                    }.count()
                    .subscribe {
                        expectedCount = it
                    }
            }
            launch {
                helper.writeBytes(request.actualDataTopic,
                    pairFlux
                        .map {
                            Triple(it.second.id.toString(), it.second.asXml().toByteArray(), it.third)
                        })
                    .doOnComplete {
                        latch.countDown()
                    }.count()
                    .subscribe {
                        actualCount = it
                    }

            }
            withContext(Dispatchers.IO) {
                latch.await()
            }
        }
        return testDataResponse {
            breakCount = breaks
            recordCount = actualCount
            assert(actualCount == expectedCount)
        }
    }

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


    override suspend fun kafkaComparison(request: PersonTopicComparison): Empty {
        if (!request.hasActualDataTopic() || !request.hasExpectedDataTopic() || request.resultTopicName.isNullOrEmpty() )
            throw IllegalArgumentException("Missing input and/or output topic arguments")
        request.partitionsToCompareList.forEach { part ->
            fun <T> keyExtractor(p: Pair<KafkaKey, T>) = p.first
            fun <T> valueExtractor(p: Pair<KafkaKey, T>) = p.second

            var matchCount = 0
            var breakCount = 0

            val actualMap: Map<KafkaKey, Person2>? = KafkaHelper(kafkaServers)
                .readPartition(kafkaConsumerGroup, request.actualDataTopic.topicName, part, readEarliest = true)
                .log()
                .map {
                    it.key() to when (request.actualDataTopic.format) {
                        KafkaTopicInfo.Format.JSON -> Person2.newBuilder().apply { fromJson(String(it.value())) }
                            .build()
                        KafkaTopicInfo.Format.XML -> Person2.newBuilder().apply { fromXML(String(it.value())) }
                            .build()
                        KafkaTopicInfo.Format.PROTO -> Person2.parseFrom(it.value())
                        else -> throw Exception("Topic format undefined")
                    }
                }
                .collectMap(::keyExtractor, ::valueExtractor)
                .block()
            val expectedMap: Map<KafkaKey, Person>? = KafkaHelper(kafkaServers)
                .readPartition(kafkaConsumerGroup, request.expectedDataTopic.topicName, part, readEarliest = true)
                .map {
                    it.key() to when (request.expectedDataTopic.format) {
                        KafkaTopicInfo.Format.JSON -> Person.newBuilder().apply { fromJson(it.value().toString()) }
                            .build()
                        KafkaTopicInfo.Format.XML -> Person.newBuilder().apply { fromXML(String(it.value())) }
                            .build()
                        KafkaTopicInfo.Format.PROTO -> Person.parseFrom(it.value())
                        else -> throw Exception("Topic format undefined")
                    }
                }
                .collectMap(::keyExtractor, ::valueExtractor)
                .block()
            if (actualMap != null && expectedMap != null) {
                actualMap.keys.intersect(expectedMap.keys).count().let {
                    println("$it records in both datasets")
                }
                actualMap.keys.minus(expectedMap.keys).count().let {
                    println("$it records only in actual dataset")
                }
                expectedMap.keys.minus(actualMap.keys).count().let {
                    println("$it records only in expected dataset")
                }

                val writeToKafka: Flux<ComparisonResult> =
                    Flux.fromIterable(actualMap.keys.intersect(expectedMap.keys))
                    .map { key ->
                        runBlocking {
                            val ret = comparePerson(personComparison {
                                expected = expectedMap[key]!!
                                actual = actualMap[key]!!
                                identifier = expected.id.toString()
                            })
                            if (ret.result == ComparisonResult.ResultType.MATCHED)
                                matchCount++
                            else
                                breakCount++
                            ret
                        }
                    }

                KafkaHelper(kafkaServers)
                    .blockWriteProto(request.resultTopicName,
                        writeToKafka
                            .map {
                                Triple(it.identifier, it, part)
                            }
                    )
                println("partition $part: $matchCount matches and $breakCount breaks")

            } else
                assert(false)
        }
        return Empty.getDefaultInstance()
    }

}

fun main(args: Array<String>) {
    HelloWorldServer.log.info("Starting up spring...")
    val app = SpringApplication(HelloWorldServer::class.java)
    app.webApplicationType = WebApplicationType.REACTIVE
    app.run(*args)
}
