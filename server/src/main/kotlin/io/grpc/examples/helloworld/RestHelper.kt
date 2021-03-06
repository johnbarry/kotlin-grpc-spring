package io.grpc.examples.helloworld

import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactor.asFlux
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

typealias JsonResponse = Mono<ServerResponse>

// numbers output as strings - so this is overly simplistic
fun GeneratedMessageV3.asJson(): String =
    JsonFormat.printer().print(this)
        .replace("\\n".toRegex(), "")
        .replace("\\r".toRegex(), "")

fun Flux<GeneratedMessageV3>.asRestResponse(): JsonResponse =
    ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            BodyInserters.fromPublisher(
                this.map { it.asJson() + "\n" },
                String::class.java
            )
        )

fun Flow<GeneratedMessageV3>.asRestResponse(): JsonResponse=
    asFlux().asRestResponse()

fun GeneratedMessageV3.asRestResponse(): JsonResponse =
    Flux.just(this).asRestResponse()

