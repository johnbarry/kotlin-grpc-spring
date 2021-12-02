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
import kotlinx.coroutines.flow.collect
import java.io.Closeable
import java.util.concurrent.TimeUnit

class HelloWorldClient(private val channel: ManagedChannel) : Closeable {
    private val stub: GreeterCoroutineStub = GreeterCoroutineStub(channel)
    private val friendServiceStub = FriendServiceGrpcKt.FriendServiceCoroutineStub(channel)

    suspend fun greet(name: String) {
        val request = helloRequest { this.name = name }
        val response = stub.sayHello(request)
        println("Received: ${response.message}")
    }

    suspend fun getFriends() =
        stub.listFriends(FriendListRequest.getDefaultInstance())
            .collect() //::println)

    suspend fun requestFriend(firstName: String, secondName: String) =
        friendServiceStub.requestFriend(friendRequest {
            firstPerson = firstName
            secondPerson = secondName
        })

    suspend fun friendRequests() =
        friendServiceStub.friendCommands(EventStreamRequest.getDefaultInstance())
            .collect {
                println( it.toString()  )
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

    val user = args.singleOrNull() ?: "world"
    client.greet(user)
    client.getFriends()
    client.requestFriend("Sally H", "Joe T")
    client.requestFriend("Harry E", "Stephanie U")

    println("List of friend requests...")
    client.friendRequests()
}
