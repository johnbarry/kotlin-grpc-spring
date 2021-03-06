/*
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.helloworld

import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

const val NEW_FRIEND_CMD_TOPIC = "NewFriendCommand"
const val NEW_FRIEND_EVENT_TOPIC = "NewFriendEvent"
const val PERSON_UPDATES_DATA_TOPIC = "PersonDataUpdates"

@SpringBootApplication
@EnableAutoConfiguration
@Component
open class HelloWorldServer : ApplicationRunner {
    private val port = 50051
    private val server: Server = ServerBuilder
        .forPort(port)
        .addService(HelloWorldService())
        .addService(FriendService())
        .build()

    private fun start() {
        server.start()
        log.info("gRPC server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                log.info("*** shutting down gRPC server since JVM is shutting down")
                this@HelloWorldServer.stop()
                log.info("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    private fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class HelloWorldService : GreeterGrpcKt.GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply = helloReply {
            message = "Hello ${request.name}"
            log.info("Client sent sayHello - responding with $message")
        }

        override fun backPressureDemo(request: BackPressureDemoRequest): Flow<ANumber> =
            (if (request.addFiller) 100 else 10_000).let { reportEvery ->
                Flux.range(1, request.number)
                    .map {
                        aNumber {
                            number = it
                            if (request.addFiller)
                                (1..it).forEach { _ -> filler.add("BlahBlahBlah") }
                        }
                    }
                    .doOnNext {
                        val n = it.number
                        if (n % reportEvery == 0)
                            log.info("Server backpressure demo: $n")
                    }
                    .asFlow()
            }
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(HelloWorldServer::class.java)
    }

    override fun run(args: ApplicationArguments?) {
        log.info("Starting grpc Server")
        val server = HelloWorldServer()
        server.start()
        server.blockUntilShutdown()
    }

}




fun main(args: Array<String>) {
    HelloWorldServer.log.info("Starting up spring...")
    val app = SpringApplication(HelloWorldServer::class.java)
    app.webApplicationType = WebApplicationType.REACTIVE
    app.run(*args)
}
