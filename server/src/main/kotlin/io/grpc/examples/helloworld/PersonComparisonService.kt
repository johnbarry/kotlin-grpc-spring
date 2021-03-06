package io.grpc.examples.helloworld

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import reactor.core.publisher.Flux


fun List<String>.multiLineAddress(): String =
    filter { it.isNotEmpty() }
        .joinToString("\n")

operator fun PersonTopicComparisonResult.Builder.plusAssign(other: PersonTopicComparisonResult.Builder) {
    matchedRecords += other.matchedRecords
    unmatchedRecords += other.unmatchedRecords
    missingExpected += other.missingExpected
    missingActual += other.missingActual
    totalRecords += other.totalRecords
}

class PersonComparisonService : PersonComparisonServiceGrpcKt.PersonComparisonServiceCoroutineImplBase(), ServiceKafkaConfig {

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

    private fun actualFlux(request: PersonTopicComparison, part: Int? = null): Flux<Pair<KafkaKey, Person2>> =
        KafkaHelper(kafkaServers)
            .let {
                if (part == null)
                    it.read(
                        kafkaConsumerName,
                        kafkaConsumerGroup,
                        request.actualDataTopic.topicName,
                        readEarliest = true
                    )
                else
                    it.readPartition(kafkaConsumerGroup, request.actualDataTopic.topicName, part, readEarliest = true)
            }
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

    private fun expectedFlux(request: PersonTopicComparison, part: Int? = null): Flux<Pair<KafkaKey, Person>> =
        KafkaHelper(kafkaServers)
            .let {
                if (part == null)
                    it.read(
                        kafkaConsumerName,
                        kafkaConsumerGroup,
                        request.expectedDataTopic.topicName,
                        readEarliest = true
                    )
                else
                    it.readPartition(kafkaConsumerGroup, request.expectedDataTopic.topicName, part, readEarliest = true)
            }
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

    override suspend fun kafkaComparison(request: PersonTopicComparison): PersonTopicComparisonResult {
        val mutex = Mutex()
        val ret = PersonTopicComparisonResult.newBuilder()

        if (!request.hasActualDataTopic() || !request.hasExpectedDataTopic() || request.resultTopicName.isNullOrEmpty())
            throw IllegalArgumentException("Missing input and/or output topic arguments")

        request.partitionsToCompareList.forEach { part ->
            fun <T> keyExtractor(p: Pair<KafkaKey, T>) = p.first
            fun <T> valueExtractor(p: Pair<KafkaKey, T>) = p.second

            val partitionResult = PersonTopicComparisonResult.newBuilder()

            val actualMap: Map<KafkaKey, Person2>? =
                actualFlux(request, part)
                    .collectMap(::keyExtractor, ::valueExtractor)
                    .block()
            val expectedMap: Map<KafkaKey, Person>? =
                expectedFlux(request, part)
                    .collectMap(::keyExtractor, ::valueExtractor)
                    .block()
            if (actualMap != null && expectedMap != null) {
                val writeToKafka: Flux<ComparisonResult> =
                    Flux.fromIterable(actualMap.keys.union(expectedMap.keys))
                        .map { key ->
                            runBlocking {
                                val expected = expectedMap[key]
                                val actual = actualMap[key]
                                partitionResult.totalRecords ++

                                (if (expected != null && actual != null) {
                                    comparePerson(personComparison {
                                        this.expected = expectedMap[key]!!
                                        this.actual = actualMap[key]!!
                                        identifier = expected.id.toString()
                                    })
                                } else {
                                    if (expected == null && actual == null)
                                        throw Exception("Unexpected NULL values for expected and actual")
                                    if (expected == null)
                                        comparisonResult {
                                            identifier = actual!!.id.toString()
                                            result = ComparisonResultType.ONLY_ACTUAL
                                        }
                                    else
                                        comparisonResult {
                                            identifier = expected.id.toString()
                                            result= ComparisonResultType.ONLY_EXPECTED
                                        }
                                }).apply {
                                    when (result) {
                                        ComparisonResultType.MATCHED -> partitionResult.matchedRecords  ++
                                        ComparisonResultType.BREAKS -> partitionResult.unmatchedRecords ++
                                        ComparisonResultType.ONLY_EXPECTED -> partitionResult.missingActual++
                                        ComparisonResultType.ONLY_ACTUAL -> partitionResult.missingExpected++
                                        else -> throw Exception("Unexpected compariosn type $result for key $key")
                                    }
                                }
                            }
                        }

                KafkaHelper(kafkaServers)
                    .blockWriteProto(request.resultTopicName,
                        writeToKafka
                            .map {
                                Triple(it.identifier, it, part)
                            }
                    )
                mutex.withLock {
                    ret += partitionResult
                    println("partition $part: ${partitionResult.matchedRecords} matches and ${partitionResult.unmatchedRecords} breaks")
                    println("totals now: ${ret.matchedRecords} matches and ${ret.unmatchedRecords} breaks")
                }

            } else
                assert(false)
        }
        return ret.build()
    }

    override fun personRecords(request: PersonRecordRequest): Flow<PersonRecordLookup> {
        val lookupBuilders = request.identifierList.associateWith {
            PersonRecordLookup.newBuilder()
        }
        actualFlux(request.topicInfo)
            .filter {
                it.first in request.identifierList
            }
            .map {
                lookupBuilders[it.first] ?. actual = it.second
                lookupBuilders[it.first] ?. identifier = it.first
            }
            .blockLast()
        expectedFlux(request.topicInfo)
            .filter {
                it.first in request.identifierList
            }
            .map {
                lookupBuilders[it.first] ?. expected = it.second
                lookupBuilders[it.first] ?. identifier = it.first
            }
            .blockLast()
        return lookupBuilders
            .values
            .mapNotNull {
                it ?. build()
            }
            .asFlow()
    }
}
