package io.grpc.examples.helloworld

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.junit.jupiter.api.*
import java.util.concurrent.atomic.AtomicLong

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Disabled
class ComparisonKafkaTest {
    private val server = PersonComparisonService()
    private val kafka = KafkaService()
    private val testData = TestDataService()
    private val result = ComparisonResultService()

    // initial set of tests with same identifiers in actual and expected
    private val actualDataTopic = "test1000.actual.xml"
    private val expectedDataTopic = "test1000.expected.proto"
    private val outputDataTopic = "test1000.result.proto"
    private val recordCount = 1000L
    private val numberOfPartitions = 10

    // later tests with differing identifiers in actual and expected
    private val missingActualDataTopic = "missingtests.actual.xml"
    private val missingExpectedDataTopic = "missingtests.expected.proto"
    private val missingOutputDataTopic = "missingtests.result.proto"

    companion object {
        var stateMatchCount: Int = 0
        var stateMismatchCount: Int = 0
        var stateActualAndExpectedCount: Int = 0
        var stateMissingActualCount: Int = 0
        var stateMissingExpectedCount: Int = 0
        var stateTotalCount: Int = 0
        fun clearTotals() {
            stateMatchCount = 0
            stateMismatchCount = 0
            stateActualAndExpectedCount = 0
            stateMissingActualCount = 0
            stateMissingExpectedCount = 0
            stateTotalCount = 0
        }
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
            println("${ret.actualCount} records created")
            assert(ret.actualCount == recordCount)
            stateMismatchCount = ret.breakCount.toInt()
            stateMatchCount = ret.actualCount.toInt() - ret.breakCount.toInt()
            stateTotalCount = ret.totalCount.toInt()
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

    private fun missingComparisonInfo(): PersonTopicComparison = personTopicComparison {
        expectedDataTopic = kafkaTopicInfo {
            topicName = this@ComparisonKafkaTest.missingExpectedDataTopic
            format = KafkaTopicInfo.Format.PROTO
        }
        actualDataTopic = kafkaTopicInfo {
            topicName = this@ComparisonKafkaTest.missingActualDataTopic
            format = KafkaTopicInfo.Format.XML
        }
        resultTopicName = missingOutputDataTopic
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
                            totals += res.toBuilder()
                        }
                    }
                }.joinAll()
                assert(totals.matchedRecords == stateMatchCount)
                assert(totals.unmatchedRecords == stateMismatchCount)
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
                assert(it.matches == stateMatchCount)
                assert(it.breaks == stateMismatchCount)
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
                        })
                    assert(cmp.result == ComparisonResultType.BREAKS)
                    assert(cmp.identifier == rec.identifier)
                }
                .count()
            assert(ct2 == number)
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
                        })
                    assert(cmp.result == ComparisonResultType.MATCHED)
                    assert(cmp.identifier == rec.identifier)
                }
                .count()
            assert(ct2 == number)
        }
    }

    @Test
    @Order(10)
    internal fun `missing data topics created`(): Unit =
        runBlocking {
            deleteTopic(missingActualDataTopic)
            delay(1000L)
            createTopic(missingActualDataTopic)
            deleteTopic(missingExpectedDataTopic)
            delay(1000L)
            createTopic(missingExpectedDataTopic)
            deleteTopic(missingOutputDataTopic)
            delay(1000L)
            createTopic(missingOutputDataTopic)
        }

    @Order(11)
    @Test
    internal fun `missing test data is generated`() =
        runBlocking {
            clearTotals()

            val ret: TestDataResponse = testData.generateTestData(
                testDataRequest {
                    records = recordCount
                    percentBreaks = 0
                    missingActualPercent = 20
                    missingExpectedPercent = 20
                    actualDataTopic = this@ComparisonKafkaTest.missingActualDataTopic
                    expectedDataTopic = this@ComparisonKafkaTest.missingExpectedDataTopic
                }
            )
            println("${ret.totalCount} records created, ${ret.actualCount} actual and ${ret.expectedCount} expected")
            stateActualAndExpectedCount = (ret.breakCount + ret.matchCount).toInt()
            stateMismatchCount = ret.breakCount.toInt()
            stateMatchCount = ret.matchCount
            stateTotalCount = ret.totalCount.toInt()
            stateMissingActualCount = ret.missingActualCount
            stateMissingExpectedCount = ret.missingExpectedCount
        }

    @Test
    @Order(12)
    internal fun `missing comparison runs`() {
        suspend fun doCompare() {
            coroutineScope {
                val mutex = Mutex()
                val totals = PersonTopicComparisonResult.newBuilder()
                (0 until numberOfPartitions).map { part ->
                    launch(Dispatchers.IO) {
                        val res: PersonTopicComparisonResult = server.kafkaComparison(
                            missingComparisonInfo().toBuilder()
                                .addPartitionsToCompare(part)
                                .build()
                        )
                        mutex.withLock {
                            totals += res.toBuilder()
                        }
                    }
                }.joinAll()
                assert(totals.matchedRecords == stateMatchCount)
                assert(totals.unmatchedRecords == stateMismatchCount)
                assert(totals.missingExpected == stateMissingExpectedCount)
                assert(totals.missingActual == stateMissingActualCount)
                assert(totals.totalRecords == stateTotalCount)
            }
        }

        runBlocking {
            doCompare()
        }
    }

    @Test
    @Order(13)
    internal fun `missing comparison populates output kafka topic`() {
        suspend fun doCount() {
            val ct = AtomicLong()
            coroutineScope {
                (0 until numberOfPartitions).map { part ->
                    launch(Dispatchers.IO) {
                        val ret = kafka.kafkaRecordCount(kafkaCountRequest {
                            topic = missingOutputDataTopic
                            partition = part
                        })
                        ct.addAndGet(ret.records)
                    }
                }.joinAll()
                assert(ct.get() == stateTotalCount.toLong())
            }
        }
        runBlocking {
            doCount()
        }
    }

    @Test
    @Order(14)
    internal fun `expected missing breakdown in output kafka topic`() {
        runBlocking {
            result.kafkaComparisonReport(
                comparisonReportTopic {
                    topicName = missingOutputDataTopic
                }
            ).let {
                assert(it.topicName == missingOutputDataTopic)
                assert(it.matches == stateMatchCount)
                assert(it.breaks == stateMismatchCount)
                assert(it.onlyActual == stateMissingExpectedCount)
                assert(it.onlyExpected == stateMissingActualCount)
            }
        }
    }

    @Test
    @Order(15)
    internal fun `check missing expected records`() {
        val number = 5
        runBlocking {

            val reqBuilder = PersonRecordRequest.newBuilder()
            reqBuilder.topicInfo = missingComparisonInfo()
            val ct = result.comparisonResult(
                comparisonReportTopic {
                    topicName = missingOutputDataTopic
                }
            )
                .filter { it.result == ComparisonResultType.ONLY_ACTUAL }
                .take(number)
                .map { brk ->
                    reqBuilder.addIdentifier(brk.identifier)
                }
                .count()
            assert(ct == number)
            val ct2 =
                server.personRecords(reqBuilder.build())
                .map { assert(it.hasActual() && !it.hasExpected())
                }
                .count()
            assert(ct2 == number)
        }
    }

    @Test
    @Order(16)
    internal fun `check missing actual records`() {
        val number = 5
        runBlocking {
            val reqBuilder = PersonRecordRequest.newBuilder()
            reqBuilder.topicInfo = missingComparisonInfo()
            val ct = result.comparisonResult(
                comparisonReportTopic {
                    topicName = missingOutputDataTopic
                }
            )
                .filter { it.result == ComparisonResultType.ONLY_EXPECTED }
                .take(number)
                .map { brk ->
                    reqBuilder.addIdentifier(brk.identifier)
                }
                .count()

            assert(ct == number)
            val ct2 = server.personRecords(reqBuilder.build())
                .map { assert(!it.hasActual() && it.hasExpected()) }
                .count()
            assert(ct2 == number)
        }
    }


}