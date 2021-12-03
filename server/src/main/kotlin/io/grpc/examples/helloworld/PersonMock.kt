package io.grpc.examples.helloworld


object PersonMock {
    fun fillOriginal(nameOnly: Person): Person = person {
        forename = nameOnly.forename
        surname = nameOnly.surname
        addressLine1 = "Rue de $forename"
        city = "$forename Ville"
    }
    fun fillUpdate(nameOnly: Person): Person = person {
        forename = nameOnly.forename
        surname = nameOnly.surname
        addressLine1 = "Neuplatz $forename"
        city = "$forename Stadt"
    }
}