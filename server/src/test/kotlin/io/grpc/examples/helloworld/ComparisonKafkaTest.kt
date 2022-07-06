package io.grpc.examples.helloworld

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Disabled
class ComparisonKafkaTest {
    private val server = ComparisonService()
    private val actualDataTopic = "test1000.actual"
    private val expectedDataTopic = "test1000.expected"
    private val recordCount = 1000L

    private fun createTopic(t: String) =
        runBlocking {
            server.createTopic(
                topicCreationRequest {
                    topic = t
                    partitions = 10
                    replicaCount = 1
                }
            )
        }

    private fun deleteTopic(t: String) =
        runBlocking {
            server.deleteTopic(
                topicDeletionRequest {
                    topic = t
                }
            )
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
                    percentBreaks= 10
                    actualDataTopic =  this@ComparisonKafkaTest.actualDataTopic
                    expectedDataTopic = this@ComparisonKafkaTest.expectedDataTopic
                }
            )
            println("${ret.recordCount} records created")
            assert(ret.recordCount == recordCount)
            println("${ret.breakCount} break created")
            assert(ret.breakCount > 0)
        }

    @Test
    @Order(3)
    internal fun `actual data topic populated correctly`() =
        runBlocking {
            val ret = server.kafkaRecordCount( kafkaCountRequest {
                topic = actualDataTopic
                allPartitions = true
            })
            println("${ret.records} actual data records populated")
            assert( ret.records == recordCount)
        }

    @Test
    @Order(4)
    internal fun `expected data topic populated correctly`() =
        runBlocking {
            val ret = server.kafkaRecordCount( kafkaCountRequest {
                topic = expectedDataTopic
                allPartitions = true
            })
            println("${ret.records} expected data records populated")
            assert( ret.records == recordCount)
        }

}