package io.grpc.examples.helloworld

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
    private val testNames by lazy {
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