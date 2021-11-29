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
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component

@SpringBootApplication
@Component
open class HelloWorldServer: CommandLineRunner {
    val port = 50051
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(HelloWorldService())
        .build()

    fun start() {
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

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class HelloWorldService : GreeterGrpcKt.GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest) = helloReply {
            message = "Hello ${request.name}"
            log.info("Client sent sayHello - responding with $message")
        }
    }

    override fun run(vararg args: String) {
        log.info("Starting grpc Server")
        val server = HelloWorldServer()
        server.start()
        server.blockUntilShutdown()
    }

    companion object {
        val log = LoggerFactory.getLogger(HelloWorldServer::class.java)
    }

}

fun main(args: Array<String>) {
    HelloWorldServer.log.info("Starting up spring...")
    val app = SpringApplication(HelloWorldServer::class.java)
    app.webApplicationType = WebApplicationType.NONE
    app.run(*args)
}
