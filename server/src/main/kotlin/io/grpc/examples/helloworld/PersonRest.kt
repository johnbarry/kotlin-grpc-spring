package io.grpc.examples.helloworld

import kotlinx.coroutines.runBlocking
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.router

@Component
class PersonHandler  {
    private val service = FriendService()

    @Suppress("UNUSED_PARAMETER")
    fun list(request: ServerRequest): JsonResponse =
            // the gRPC service call ...
            service.listPeople().asRestResponse()

    fun get(request: ServerRequest): JsonResponse =
        runBlocking {
                // the gRPC service call ...
                service.getPerson(
                    personId {
                        id = request.pathVariable("id").toLong()
                    })
                    .asRestResponse()
        }
}


@Configuration
open class PersonRouter(private val handler: PersonHandler) {

    @Bean
    open fun theRouter() = router {
        accept(MediaType.APPLICATION_JSON).nest {
            GET("/people").invoke(handler::list)
            GET("/person/{id}").invoke(handler::get)
        }
    }
}