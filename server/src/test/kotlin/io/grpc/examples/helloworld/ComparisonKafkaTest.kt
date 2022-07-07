package io.grpc.examples.helloworld

import kotlinx.coroutines.*
import org.junit.jupiter.api.*

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Disabled
class ComparisonKafkaTest {
    private val server = ComparisonService()
    private val actualDataTopic = "test1000.actual.xml"
    private val expectedDataTopic = "test1000.expected.proto"
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

    private fun deleteTopic(t: String) = try {
        runBlocking {
            server.deleteTopic(
                topicDeletionRequest {
                    topic = t
                }
            )
        }

    } catch (_: Exception) {

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
                (0 until numberOfPartitions).forEach { part ->
                    launch(Dispatchers.IO) {
                        println("partition $part check")
                        val ret = server.kafkaRecordCount(kafkaCountRequest {
                            topic = expectedDataTopic
                            partition = part
                        })
                        println("partition $part has ${ret.records} records")
                        assert (ret.records == recordCount / numberOfPartitions)
                    }
                }
            }
        }
        runBlocking {
            doIt()
        }
    }

    @Test
    @Order(6)
    internal fun `comparison runs`(): Unit =
        runBlocking {
            server.kafkaComparison(personTopicComparison {
                expectedDataTopic = kafkaTopicInfo {
                    topicName = this@ComparisonKafkaTest.expectedDataTopic
                    format = KafkaTopicInfo.Format.XML
                }
                actualDataTopic = kafkaTopicInfo {
                    topicName = this@ComparisonKafkaTest.actualDataTopic
                    format = KafkaTopicInfo.Format.PROTO
                }
                partitionsToCompare.add(0)
                partitionsToCompare.add(1)
            })
        }

}