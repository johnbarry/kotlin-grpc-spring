package io.grpc.examples.helloworld

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux

class ComparisonResultService : ComparisonResultServiceGrpcKt.ComparisonResultServiceCoroutineImplBase(), ServiceKafkaConfig {

    override fun comparisonResult(request: ComparisonReportTopic): Flow<ComparisonResult> =
        KafkaHelper(kafkaServers)
            .read(kafkaConsumerName, kafkaConsumerGroup, request.topicName, readEarliest = true)
            .map {
                ComparisonResult.parseFrom(it.value())
            }.asFlow()

    override suspend fun kafkaComparisonReport(request: ComparisonReportTopic): ComparisonSummary {
        val summaryBuilder = ComparisonSummary.newBuilder()
        summaryBuilder.topicName = request.topicName

        summaryBuilder.totalRecords = comparisonResult(request)
            .asFlux()
            .map {
                when (it.result) {
                    ComparisonResultType.MATCHED -> summaryBuilder.matches++
                    ComparisonResultType.BREAKS -> summaryBuilder.breaks++
                    ComparisonResultType.ONLY_ACTUAL -> summaryBuilder.onlyActual++
                    ComparisonResultType.ONLY_EXPECTED -> summaryBuilder.onlyExpected++
                    else -> throw java.lang.Exception("Missing status for record ${it.identifier}")
                }
            }.count()
            .block()?.toInt() ?: 0


        return summaryBuilder.build()
    }

}
