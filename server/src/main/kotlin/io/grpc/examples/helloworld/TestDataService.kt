package io.grpc.examples.helloworld

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import reactor.core.publisher.Flux
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

class TestDataService : TestDataServiceGrpcKt.TestDataServiceCoroutineImplBase(), ServiceKafkaConfig {
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
}


