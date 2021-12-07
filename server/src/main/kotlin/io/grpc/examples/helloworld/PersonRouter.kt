package io.grpc.examples.helloworld

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.web.reactive.function.server.router


@Configuration
open class PersonRouter(private val handler: PersonHandler) {

    @Bean
    open fun theRouter() = router {
        (accept(APPLICATION_JSON) and "/people").invoke ( handler::listPeople )
    }
}