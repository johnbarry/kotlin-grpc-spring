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


import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.examples.helloworld.GreeterGrpcKt.GreeterCoroutineStub
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.TimeUnit

class HelloWorldClient(private val channel: ManagedChannel) : Closeable {
    private val stub: GreeterCoroutineStub = GreeterCoroutineStub(channel)
    private val friendServiceStub = FriendServiceGrpcKt.FriendServiceCoroutineStub(channel)

    suspend fun greet(name: String) {
        val request = helloRequest { this.name = name }
        val response = stub.sayHello(request)
        println("Received: ${response.message}")
    }

    suspend fun listPeople(surnameContains: String? = null, limit: Int? = null, operation: PersonChange.Operation? = null) {
        friendServiceStub.listPeopleChanges(Empty.getDefaultInstance())
            .let { base ->
                surnameContains?.let {
                    base.filter { p -> p.person.surname.contains(it) }
                } ?: base
            }
            .let { base ->
                operation?.let {
                    base.filter { p -> p.operation  == it }
                } ?: base
            }
            .let { filtered ->
                limit?.let {
                    filtered.take(it)
                } ?: filtered
            }
            .collect { println(it) }
    }

    suspend fun requestFriend(person1: Pair<String, String>, person2: Pair<String, String>) =
        friendServiceStub.requestFriend(friendRequest {
            firstPerson = person { forename = person1.first; surname = person1.second }
            secondPerson = person { forename = person2.first; surname = person2.second }
        })

    suspend fun friendRequests() =
        friendServiceStub.friendCommands(EventStreamRequest.getDefaultInstance())
            .collect {
                println(it.toString())
            }

    suspend fun makeOutstandingFriends() =
        friendServiceStub.makeOutstandingFriends(EventStreamRequest.getDefaultInstance())
            .collect {
                println(it.toString())
            }

    suspend fun generateTestData() = friendServiceStub.generateTestData(Empty.getDefaultInstance())

    suspend fun firstCallback()  {
        val firstOne = friendServiceStub.peopleChangeEvents(Empty.getDefaultInstance()).first()
        val res = friendServiceStub.changeCallback(firstOne)
        println("First change was $firstOne")
        println("Details: $res")
    }

    suspend fun backpressureDemo(countTo: Int=100000, delayMillis: Long = 10, withFiller: Boolean = true) =
        stub.backPressureDemo ( backPressureDemoRequest {
            number = countTo
            addFiller = withFiller
        })
            .collect {
                val n = it.number
                if (n % 100 == 1)
                    println("Client backpressure demo: $n")
                if (delayMillis >0) delay(delayMillis)
            }





    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

}

/**
 * Greeter, uses first argument as name to greet if present;
 * greets "world" otherwise.
 */
suspend fun main(args: Array<String>) {
    val port = 50051

    val channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build()

    val client = HelloWorldClient(channel)

    println("=========> Small message no client delay")
    client.backpressureDemo(countTo = 500_000, delayMillis = 0, withFiller = false)

    println("=========> Large message 10ms client delay")
    client.backpressureDemo(countTo = 2_000, delayMillis = 10, withFiller = true)

    return

    client.generateTestData()

    client.firstCallback()

    println("First 5 creations ...")
    client.listPeople(limit = 5, operation=PersonChange.Operation.CREATE)

    println("=======================================================")

    println("First 5 B-B updates...")
    client.listPeople(surnameContains = "B-B", limit = 5, operation=PersonChange.Operation.UPDATE)
    println("=======================================================")

    return

    val user = args.singleOrNull() ?: "world"
    client.greet(user)
    client.requestFriend(Pair("Sally", "H"), Pair("Joe", "T"))
    client.requestFriend(Pair("Harry", "E"), Pair("Stephanie", "U"))

    println("List of friend requests...")
    client.friendRequests()
    println(".. List of friend requests")

    println("Execute friend requests, returns events...")
    client.makeOutstandingFriends()
}
