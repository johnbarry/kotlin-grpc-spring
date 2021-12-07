package io.grpc.examples.helloworld


import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.reactor.asFlux
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

fun Person.asJson(): String = JsonFormat.printer().print(this)

@Component
class PersonHandler {
    @Suppress("UNUSED_PARAMETER")
    fun listPeople(request: ServerRequest): Mono<ServerResponse> =
        PersonMock.testNames.asFlow().asFlux()
            .map{ it.asJson() + "\n"}
            .let { dataStream ->
            ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromPublisher(dataStream, String::class.java))
        }
}