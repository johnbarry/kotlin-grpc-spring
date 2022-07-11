package io.grpc.examples.helloworld

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.junit.jupiter.api.*

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Disabled
class ComparisonKafkaTest {
    private val server = PersonComparisonService()
    private val kafka = KafkaService()
    private val testData = TestDataService()
    private val result = ComparisonResultService()
    private val actualDataTopic = "test1000.actual.xml"
    private val expectedDataTopic = "test1000.expected.proto"
    private val outputDataTopic = "test1000.result.proto"
    private val recordCount = 1000L
    private val numberOfPartitions = 10

    companion object {
        var expectedMatches: Int = 0
        var expectedMismatches: Int = 0
    }

    private fun createTopic(t: String) =
        runBlocking {
            kafka.createTopic(
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
                kafka.deleteTopic(
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
            val ret: TestDataResponse = testData.generateTestData(
                testDataRequest {
                    records = recordCount
                    percentBreaks = 10
                    actualDataTopic = this@ComparisonKafkaTest.actualDataTopic
                    expectedDataTopic = this@ComparisonKafkaTest.expectedDataTopic
                }
            )
            println("${ret.recordCount} records created")
            assert(ret.recordCount == recordCount)
            expectedMismatches = ret.breakCount.toInt()
            expectedMatches = ret.recordCount.toInt() - ret.breakCount.toInt()
            println("${ret.breakCount} breaks created")
            assert(ret.breakCount > 0)
        }

    @Test
    @Order(3)
    internal fun `actual data topic populated correctly`() =
        runBlocking {
            val ret = kafka.kafkaRecordCount(kafkaCountRequest {
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
            val ret = kafka.kafkaRecordCount(kafkaCountRequest {
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
                            val ret = kafka.kafkaRecordCount(kafkaCountRequest {
                                topic = expectedDataTopic
                                partition = part
                            })
                            assert(ret.records == recordCount / numberOfPartitions)
                        },
                        launch(Dispatchers.IO) {
                            val ret = kafka.kafkaRecordCount(kafkaCountRequest {
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

    private operator fun PersonTopicComparisonResult.Builder.plusAssign(other: PersonTopicComparisonResult) {
        matchedRecords += other.matchedRecords
        unmatchedRecords += other.unmatchedRecords
    }

    private fun comparisonInfo(): PersonTopicComparison = personTopicComparison {
        expectedDataTopic = kafkaTopicInfo {
            topicName = this@ComparisonKafkaTest.expectedDataTopic
            format = KafkaTopicInfo.Format.PROTO
        }
        actualDataTopic = kafkaTopicInfo {
            topicName = this@ComparisonKafkaTest.actualDataTopic
            format = KafkaTopicInfo.Format.XML
        }
        resultTopicName = outputDataTopic
    }

    @Test
    @Order(6)
    internal fun `comparison runs`() {
        suspend fun doCompare() {
            coroutineScope {
                val mutex = Mutex()
                val totals = PersonTopicComparisonResult.newBuilder()
                (0 until numberOfPartitions).map { part ->
                    launch(Dispatchers.IO) {
                        val res: PersonTopicComparisonResult = server.kafkaComparison(
                            comparisonInfo().toBuilder()
                                .addPartitionsToCompare(part)
                                .build()
                        )
                        mutex.withLock {
                            totals += res
                        }
                    }
                }.joinAll()
                assert(totals.matchedRecords == expectedMatches)
                assert(totals.unmatchedRecords == expectedMismatches)
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
                        val ret = kafka.kafkaRecordCount(kafkaCountRequest {
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

    @Test
    @Order(7)
    internal fun `expected breakdown in output kafka topic`() {
        runBlocking {
            result.kafkaComparisonReport(
                comparisonReportTopic {
                    topicName = outputDataTopic
                }
            ).let {
                assert(it.topicName == outputDataTopic)
                assert(it.matches == expectedMatches)
                assert(it.breaks == expectedMismatches)
                assert(it.onlyActual == 0)
                assert(it.onlyExpected == 0)
            }
        }
    }

    @Test
    @Order(8)
    internal fun `check five mismatches`() {
        val number = 5
        runBlocking {
            val reqBuilder = PersonRecordRequest.newBuilder()
            reqBuilder.topicInfo = comparisonInfo()
            val ct = result.comparisonResult(
                comparisonReportTopic {
                    topicName = outputDataTopic
                }
            )
                .filter { it.result == ComparisonResultType.BREAKS }
                .take(number)
                .map { brk ->
                    reqBuilder.addIdentifier(brk.identifier)
                }
                .count()
            assert(ct == number)
            val ct2 = server.personRecords(reqBuilder.build())
                .map { rec ->
                    val cmp = server.comparePerson(
                        personComparison {
                            actual = rec.actual
                            expected = rec.expected
                            identifier = rec.identifier
                        } )
                    assert(cmp.result == ComparisonResultType.BREAKS)
                    assert(cmp.identifier == rec.identifier)
                }
                .count()
            assert(ct2 == 5)
        }
    }

    @Test
    @Order(9)
    internal fun `check five matches`() {
        val number = 5
        runBlocking {
            val reqBuilder = PersonRecordRequest.newBuilder()
            reqBuilder.topicInfo = comparisonInfo()
            val ct = result.comparisonResult(
                comparisonReportTopic {
                    topicName = outputDataTopic
                }
            )
                .filter { it.result == ComparisonResultType.MATCHED }
                .take(number)
                .map { brk ->
                    reqBuilder.addIdentifier(brk.identifier)
                }
                .count()
            assert(ct == number)
            val ct2 = server.personRecords(reqBuilder.build())
                .map { rec ->
                    val cmp = server.comparePerson(
                        personComparison {
                            actual = rec.actual
                            expected = rec.expected
                            identifier = rec.identifier
                        } )
                    assert(cmp.result == ComparisonResultType.MATCHED)
                    assert(cmp.identifier == rec.identifier)
                }
                .count()
            assert(ct2 == 5)
        }
    }
}