package io.grpc.examples.helloworld

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import reactor.core.publisher.Flux
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

data class TestRecord(val expected: Person?, val actual: Person2?, val partition: Int, val mismatch: Boolean)

class TestDataService : TestDataServiceGrpcKt.TestDataServiceCoroutineImplBase(), ServiceKafkaConfig {
    override suspend fun generateTestData(request: TestDataRequest): TestDataResponse {
        var breaks = 0L
        val helper = KafkaHelper(kafkaServers)
        val ret = TestDataResponse.newBuilder()
        val fluxConsumers = 3 // number of 'launch' below - used to set up latch
        val pairFlux: Flux <TestRecord> = Flux.fromIterable(1..request.records)
            .map {
                val id = 1L + it
                val partition = id.toInt() % numberPartitions
                with(PersonFaker.fakePersonPair(id)) {
                    val person: Person? = if (request.missingExpectedPercent == 0 || Random.nextInt(100) < 100 - request.missingExpectedPercent)
                        first
                    else
                        null
                    val person2: Person2? = if (request.missingActualPercent == 0 || Random.nextInt(100) < 100 - request.missingActualPercent)
                        second
                    else null

                    if (request.percentBreaks > 0 && Random.nextInt(100) <= request.percentBreaks && person2 != null && person != null)
                        TestRecord(
                            person,
                            second.toBuilder()
                                .addAddress("extra address line for break ${++breaks}")
                                .setName(
                                    if (Random.nextInt(100) >= 50)
                                        second.name + " XX"
                                    else second.name
                                )
                                .build(),
                            partition,
                            mismatch = true
                        )
                    else
                        TestRecord(person, person2, partition, mismatch = false)
                }
            }
            .publish()
            .refCount(fluxConsumers)
        val latch = CountDownLatch(fluxConsumers)

        coroutineScope {
            launch {
                helper.writeProto(request.expectedDataTopic,
                    pairFlux
                        .filter {
                            it.expected != null
                        }
                        .map {
                            Triple(it.expected!!.id.toString(), it.expected, it.partition)
                        }
                )
                    .doOnComplete {
                        latch.countDown()
                    }.count()
                    .subscribe {
                        ret.expectedCount = it
                    }
            }
            launch {
                helper.writeBytes(request.actualDataTopic,
                    pairFlux
                        .filter {
                            it.actual != null
                        }
                        .map {
                            Triple(it.actual!!.id.toString(), it.actual.asXml().toByteArray(), it.partition)
                        })
                    .doOnComplete {
                        latch.countDown()
                    }.count()
                    .subscribe {
                        ret.actualCount = it
                    }
            }
            launch {
                pairFlux
                    .map {
                        if (it.actual != null || it.expected != null)
                            ret.totalCount++

                        if (it.actual != null && it.expected != null) {
                            if (it.mismatch)
                                ret.breakCount++
                            else
                                ret.matchCount++
                        }
                        if (it.actual == null && it.expected != null)
                            ret.missingActualCount++
                        else {
                            if (it.expected == null && it.actual != null)
                                ret.missingExpectedCount++
                        }
                        Unit
                    }
                    .doOnComplete {
                        latch.countDown()
                    }.count()
                    .block()
            }
            withContext(Dispatchers.IO) {
                latch.await()
            }
        }
        return ret.build()
    }
}


