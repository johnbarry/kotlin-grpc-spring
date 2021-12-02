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
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

const val FRIEND_REQUEST_TOPIC = "FriendRequest"

@SpringBootApplication
@Component
open class HelloWorldServer : CommandLineRunner {
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

        override fun listFriends(request: FriendListRequest): Flow<FriendReply> =
            Flux.fromIterable(
                listOf("Harry", "Sally", "Joe", "Mary", "Ted", "Jack", "Stephanie", "Steven")
                    .flatMap { name -> ('A'..'Z').map { x -> "$name $x" } }
            )
                .map { friendReply { name = it } }
                .asFlow()
    }

    private class FriendService : FriendServiceGrpcKt.FriendServiceCoroutineImplBase() {

        data class ParseException(val key: String, val body: ByteArray) : Exception() {
            override fun toString(): String =
                "parse error for key: $key, data = [[$body]]"

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as ParseException

                if (key != other.key) return false
                if (!body.contentEquals(other.body)) return false

                return true
            }

            override fun hashCode(): Int {
                var result = key.hashCode()
                result = 31 * result + body.contentHashCode()
                return result
            }

        }

        @Suppress("BlockingMethodInNonBlockingContext")
        override suspend fun requestFriend(request: FriendRequest): NewFriendCommand =
            newFriendCommand {
                firstPerson = request.firstPerson
                secondPerson = request.secondPerson
            }.apply {
                KafkaCommandEventHelper.write(
                    FRIEND_REQUEST_TOPIC,
                    Flux.just(Pair("$firstPerson 2 $secondPerson", this))
                )
            }

        override fun friendCommands(request: EventStreamRequest): Flow<NewFriendCommand> =
            KafkaCommandEventHelper.read(
                consumerName = "friendRequests-list",
                groupName = "friendRequests-list-grp",
                topicName = FRIEND_REQUEST_TOPIC,
                readEarliest = true
            )
                .log(null, java.util.logging.Level.FINE)
                .map {
                    log.info("offset ${it.offset()}: ${it.key()}")
                    try {
                        NewFriendCommand.parseFrom(it.value())
                            .toBuilder()
                            .setKafkaOffset(it.offset())
                            .build()
                    } catch (ex: Exception) {
                        throw ParseException(it.key(), it.value())
                    }
                }
                .onErrorContinue { t: Throwable, _: Any  ->
                    log.error( t.toString() )
                }
                .asFlow()


    }

    override fun run(vararg args: String) {
        log.info("Starting grpc Server")
        val server = HelloWorldServer()
        server.start()
        server.blockUntilShutdown()
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(HelloWorldServer::class.java)
    }

}

fun main(args: Array<String>) {
    HelloWorldServer.log.info("Starting up spring...")
    val app = SpringApplication(HelloWorldServer::class.java)
    app.webApplicationType = WebApplicationType.NONE
    app.run(*args)
}
