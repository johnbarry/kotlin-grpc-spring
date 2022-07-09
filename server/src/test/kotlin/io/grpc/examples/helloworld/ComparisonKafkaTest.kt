package io.grpc.examples.helloworld

import kotlinx.coroutines.*
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.junit.jupiter.api.*

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Disabled
class ComparisonKafkaTest {
    private val server = ComparisonService()
    private val actualDataTopic = "test1000.actual.xml"
    private val expectedDataTopic = "test1000.expected.proto"
    private val outputDataTopic = "test1000.result.proto"
    private val recordCount = 1000L
    private val numberOfPartitions = 10

    private fun createTopic(t: String) =
        runBlocking {
            server.createTopic(
                topicCreationRequest {
                    topic = t
                    partitions = numberOfPartitions
                    replicaCount = 1
                }
            )
        }

    private fun deleteTopic(t: String) {
        try {
            runBlocking {
                server.deleteTopic(
                    topicDeletionRequest {
                        topic = t
                    }
                )
            }
        } catch (ex: Exception) {
            if (ex.cause !is UnknownTopicOrPartitionException)
                throw ex
        }
    }

    @Test
    @Order(1)
    internal fun `data topics created`(): Unit =
        runBlocking {
            deleteTopic(actualDataTopic)
            delay(1000L)
            createTopic(actualDataTopic)
            deleteTopic(expectedDataTopic)
            delay(1000L)
            createTopic(expectedDataTopic)
            deleteTopic(outputDataTopic)
            delay(1000L)
            createTopic(outputDataTopic)
        }

    @Order(2)
    @Test
    internal fun `test data is generated`() =
        runBlocking {
            val ret = server.generateTestData(
                testDataRequest {
                    records = recordCount
                    percentBreaks = 10
                    actualDataTopic = this@ComparisonKafkaTest.actualDataTopic
                    expectedDataTopic = this@ComparisonKafkaTest.expectedDataTopic
                }
            )
            println("${ret.recordCount} records created")
            assert(ret.recordCount == recordCount)
            println("${ret.breakCount} breaks created")
            assert(ret.breakCount > 0)
        }

    @Test
    @Order(3)
    internal fun `actual data topic populated correctly`() =
        runBlocking {
            val ret = server.kafkaRecordCount(kafkaCountRequest {
                topic = actualDataTopic
                allPartitions = true
            })
            println("${ret.records} actual data records populated")
            assert(ret.records == recordCount)
        }

    @Test
    @Order(4)
    internal fun `expected data topic populated correctly`() =
        runBlocking {
            val ret = server.kafkaRecordCount(kafkaCountRequest {
                topic = expectedDataTopic
                allPartitions = true
            })
            println("${ret.records} expected data records populated")
            assert(ret.records == recordCount)
        }


    @Test
    @Order(5)
    internal fun `10 partitions of 100 rows`() {
        suspend fun doIt() {
            coroutineScope {
                (0 until numberOfPartitions).flatMap { part ->
                    listOf(
                        launch(Dispatchers.IO) {
                            val ret = server.kafkaRecordCount(kafkaCountRequest {
                                topic = expectedDataTopic
                                partition = part
                            })
                            assert(ret.records == recordCount / numberOfPartitions)
                        },
                        launch(Dispatchers.IO) {
                            val ret = server.kafkaRecordCount(kafkaCountRequest {
                                topic = actualDataTopic
                                partition = part
                            })
                            assert(ret.records == recordCount / numberOfPartitions)
                        })
                }.joinAll()
            }
        }
        runBlocking {
            doIt()
        }
    }

    @Test
    @Order(6)
    internal fun `comparison runs`() {
        suspend fun doCompare() {
            coroutineScope {
                (0 until numberOfPartitions).map { part ->
                    launch(Dispatchers.IO) {
                        server.kafkaComparison(personTopicComparison {
                            expectedDataTopic = kafkaTopicInfo {
                                topicName = this@ComparisonKafkaTest.expectedDataTopic
                                format = KafkaTopicInfo.Format.PROTO
                            }
                            actualDataTopic = kafkaTopicInfo {
                                topicName = this@ComparisonKafkaTest.actualDataTopic
                                format = KafkaTopicInfo.Format.XML
                            }
                            resultTopicName = outputDataTopic
                            partitionsToCompare.add(part)
                        })
                    }
                }.joinAll()
            }
        }

        runBlocking {
            doCompare()
        }

    }
    @Test
    @Order(6)
    internal fun `comparison populates output kafka topic`() {
        suspend fun doCount() {
            coroutineScope {
                (0 until numberOfPartitions).map { part ->
                    launch(Dispatchers.IO) {
                        val ret = server.kafkaRecordCount(kafkaCountRequest {
                            topic = outputDataTopic
                            partition = part
                        })
                        assert(ret.records == recordCount / numberOfPartitions)
                    }
                }.joinAll()
            }
        }

        runBlocking {
            doCount()
        }

    }
}