package io.grpc.examples.helloworld

import net.datafaker.Faker
import java.util.UUID

fun Person.creation() = this.let { x ->
    personChange {
        changeId = UUID.randomUUID().toString()
        person = person {
            forename = x.forename
            surname = x.surname
            addressLine1 = "Rue de $forename"
            city = "$forename Ville"
        }
        operation = PersonChange.Operation.CREATE
        version = 1
    }
}

fun Person.update() = this.let { x ->
    personChange {
        changeId = UUID.randomUUID().toString()
        person = person {
            forename = x.forename
            surname = x.surname
            addressLine1 = "$forename Neuplatz"
            city = "$forename Stadt"
        }
        operation = PersonChange.Operation.UPDATE
        version = 2
    }
}

object PersonMock {
    val testNames: Sequence<Person> by lazy {
        ('A'..'Z').let { a2z ->
            a2z.flatMap { a -> a2z.map { b -> "$a-$b" } }
        }.let { surnames ->
            sequenceOf("Harry", "Sally", "Joe", "Mary", "Ted", "Jack", "Stephanie", "Steven")
                .flatMap { f -> surnames.map { s -> Pair(f, s) } }
                .mapIndexed{ i, p ->  person { forename = p.first; surname = p.second; id = i.toLong() } }
        }
    }
    val originalRecords: Sequence<PersonChange> by lazy { testNames.map { it.creation() } }
    val updatedRecords: Sequence<PersonChange> by lazy { testNames.map { it.update() } }
}

object PersonFaker {

    private val nameFaker = Faker().name()
    private val addressFaker = Faker().address()
    fun fakePersonPair(id: Long): Pair<Person,Person2> {
        val b1 = Person.newBuilder()
        val b2 = Person2.newBuilder()

        b1.id = id
        b2.id = id
        b1.forename = nameFaker.firstName()
        b1.surname = nameFaker.lastName()
        b2.name = "${b1.forename} ${b1.surname}"
        b1.addressLine1 = addressFaker.streetAddress()
        b1.city = addressFaker.city()
        b2.addAddress( b1.addressLine1)
        b2.addAddress( b1.city )

        return Pair(b1.build(), b2.build())
    }
}