package io.grpc.examples.helloworld

import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import reactor.core.publisher.Flux
import java.time.Instant

class FriendService : FriendServiceGrpcKt.FriendServiceCoroutineImplBase() {

    private val kafkaServers = "localhost:9092"

    data class ParseException(val key: String, val offset: Long) : Exception() {
        override fun toString(): String = "parse error for key: $key, offset=$offset"
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override suspend fun requestFriend(request: FriendRequest): NewFriendCommand =
        newFriendCommand {
            firstPerson = request.firstPerson
            secondPerson = request.secondPerson
        }.apply {
            KafkaHelper(kafkaServers).writeSinglePartition(NEW_FRIEND_CMD_TOPIC, Flux.just(Pair(null, this)))
        }

    private fun timeNow(): Timestamp = timestamp { seconds = Instant.now().epochSecond }

    @Suppress("UNUSED_PARAMETER")
    fun friendCommandsFlux(
        request: EventStreamRequest,
        consumer: String,
        group: String,
        readEarliest: Boolean
    ): Flux<NewFriendCommand> =
        KafkaHelper(kafkaServers).read(consumer, group, NEW_FRIEND_CMD_TOPIC, readEarliest)
            .map {
                try {
                    NewFriendCommand.parseFrom(it.value())
                        .toBuilder()
                        .setKafkaOffset(it.offset())
                        .build()
                        .apply {
                            HelloWorldServer.log.debug("friend command created: offset ${it.offset()}: key ${it.key()}")
                        }
                } catch (ex: Exception) {
                    throw ParseException(it.key(), it.offset())
                }
            }
            .onErrorContinue { t: Throwable, _: Any ->
                HelloWorldServer.log.error(t.toString())
            }

    override fun friendCommands(request: EventStreamRequest): Flow<NewFriendCommand> =
        friendCommandsFlux(request, "friendCommands-list", "friendCommands-list-grp", true).asFlow()

    override suspend fun makeFriend(request: NewFriendCommand): NewFriendshipEvent = cmdMakeFriend(request)

    private fun cmdMakeFriend(request: NewFriendCommand): NewFriendshipEvent =
        newFriendshipEvent {
            firstPerson = request.firstPerson
            secondPerson = request.secondPerson
            dated = timeNow()
        }.apply {
            KafkaHelper(kafkaServers).writeSinglePartition(NEW_FRIEND_EVENT_TOPIC, Flux.just(Pair(null, this)))
        }

    override fun makeOutstandingFriends(request: EventStreamRequest): Flow<NewFriendshipEvent> =
        friendCommandsFlux(request, "friendCommands-exec", "friendCommands-exec-grp", true)
            .map { cmdMakeFriend(it) }
            .asFlow()

    override suspend fun generateTestData(request: Empty): Empty {
        KafkaHelper(kafkaServers).writeSinglePartition(PERSON_UPDATES_DATA_TOPIC,
            (PersonMock.originalRecords + PersonMock.updatedRecords)
                .asFlow()
                .asFlux()
                .log()
                .map { p -> Pair(p.changeId.toString(), p) }
        )
        return Empty.getDefaultInstance()
    }

    private fun peopleUpdateFlux() =
        KafkaHelper(kafkaServers)
            .read("person-list", "person-list-group", PERSON_UPDATES_DATA_TOPIC, true)
            .map { PersonChange.parseFrom(it.value()) }

    override fun listPeopleChanges(request: Empty): Flow<PersonChange> =
        peopleUpdateFlux().asFlow()

    override fun listPeople(request: Empty): Flow<Person> =
        PersonMock.testNames.asFlow()

    fun listPeople(): Flow<Person> = listPeople(Empty.getDefaultInstance())

    override suspend fun getPerson(request: PersonId): Person =
        listPeople(Empty.getDefaultInstance())
            .filter { p -> p.id == request.id }
            .firstOrNull() ?: Person.getDefaultInstance()

    override suspend fun changeCallback(request: PersonChangeEvent): PersonChange =
        peopleUpdateFlux()
            .filter { it.changeId == request.changeId }
            .blockFirst() ?: PersonChange.getDefaultInstance()

    override fun peopleChangeEvents(request: Empty): Flow<PersonChangeEvent> =
        peopleUpdateFlux()
            .map { p ->
                personChangeEvent {
                    changeId = p.changeId
                    id = p.person.id
                    operation = p.operation
                }
            }
            .asFlow()

}
