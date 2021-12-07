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

import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.*
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import java.time.Instant

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
                    .map{ aNumber {
                        number = it
                        if (request.addFiller)
                            (1..it).forEach { _ -> filler.add("BlahBlahBlah") }
                    } }
                    .doOnNext {
                        val n = it.number
                        if (n % reportEvery == 0)
                            log.info("Server backpressure demo: $n")
                    }
                    .asFlow()
            }
    }

    private class FriendService : FriendServiceGrpcKt.FriendServiceCoroutineImplBase() {

        data class ParseException(val key: String, val offset: Long) : Exception() {
            override fun toString(): String = "parse error for key: $key, offset=$offset"
        }

        @Suppress("BlockingMethodInNonBlockingContext")
        override suspend fun requestFriend(request: FriendRequest): NewFriendCommand =
            newFriendCommand {
                firstPerson = request.firstPerson
                secondPerson = request.secondPerson
            }.apply {
                KafkaCommandEventHelper.write(NEW_FRIEND_CMD_TOPIC, Flux.just(Pair(null, this)))
            }

        private fun timeNow(): Timestamp = timestamp { seconds = Instant.now().epochSecond }

        @Suppress("UNUSED_PARAMETER")
        fun friendCommandsFlux(
            request: EventStreamRequest,
            consumer: String,
            group: String,
            readEarliest: Boolean
        ): Flux<NewFriendCommand> =
            KafkaCommandEventHelper.read(consumer, group, NEW_FRIEND_CMD_TOPIC, readEarliest)
                .map {
                    try {
                        NewFriendCommand.parseFrom(it.value())
                            .toBuilder()
                            .setKafkaOffset(it.offset())
                            .build()
                            .apply {
                                log.debug("friend command created: offset ${it.offset()}: key ${it.key()}")
                            }
                    } catch (ex: Exception) {
                        throw ParseException(it.key(), it.offset())
                    }
                }
                .onErrorContinue { t: Throwable, _: Any ->
                    log.error(t.toString())
                }

        override fun friendCommands(request: EventStreamRequest): Flow<NewFriendCommand> =
            friendCommandsFlux(request, "friendCommands-list", "friendCommands-list-grp", true).asFlow()

        override suspend fun makeFriend(request: NewFriendCommand): NewFriendshipEvent = cmdMakeFriend(request)

        fun cmdMakeFriend(request: NewFriendCommand): NewFriendshipEvent =
            newFriendshipEvent {
                firstPerson = request.firstPerson
                secondPerson = request.secondPerson
                dated = timeNow()
            }.apply {
                KafkaCommandEventHelper.write(NEW_FRIEND_EVENT_TOPIC, Flux.just(Pair(null, this)))
            }

        override fun makeOutstandingFriends(request: EventStreamRequest): Flow<NewFriendshipEvent> =
            friendCommandsFlux(request, "friendCommands-exec", "friendCommands-exec-grp", true)
                .map { cmdMakeFriend(it) }
                .asFlow()

        override suspend fun generateTestData(request: Empty): Empty {
            KafkaCommandEventHelper.write(PERSON_UPDATES_DATA_TOPIC,
                (PersonMock.originalRecords + PersonMock.updatedRecords)
                    .asFlow()
                    .asFlux()
                    .log()
                    .map { p -> Pair(p.changeId.toString(), p) }
            )
            return Empty.getDefaultInstance()
        }

        private fun peopleUpdateFlux() =
            KafkaCommandEventHelper.read("person-list", "person-list-group", PERSON_UPDATES_DATA_TOPIC, true)
                .map { PersonChange.parseFrom(it.value()) }

        override fun listPeopleChanges(request: Empty): Flow<PersonChange> =
            peopleUpdateFlux().asFlow()

        override suspend fun changeCallback(request: PersonChangeEvent): PersonChange =
            peopleUpdateFlux()
                .filter { it.changeId == request.changeId  }
                .blockFirst() ?: PersonChange.getDefaultInstance()

        override fun peopleChangeEvents(request: Empty): Flow<PersonChangeEvent> =
            peopleUpdateFlux()
                .map { p -> personChangeEvent {
                   changeId = p.changeId
                   id = p.person.id
                   operation = p.operation
                }}
                .asFlow()


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
