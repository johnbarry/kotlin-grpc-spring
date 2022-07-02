package io.grpc.examples.helloworld

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class ComparisonTest {

    private val server = ComparisonService()

    private val jsonExpected1 = person {
        id = 50
        forename = "Joe"
        surname = "Bloggs"
        addressLine1 = "123 Main Street"
        city = "Cary"
    }
    private val jsonActual1 = person2 {
        id = 50
        name = "Joe Bloggs"
        address.add("123 Main Street")
        address.add("Cary")
    }
    private val jsonActual2 = jsonActual1.toBuilder()
        .setName("XXX")
        .build()
    private val jsonActual3 = jsonActual1.toBuilder()
        .addAddress("extra address line")
        .build()

    @Test
    fun personMatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = jsonActual1
                expected = jsonExpected1
            } )
            assert(res.breaksCount == 0)

        }
    }

    @Test
    fun personNameMismatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = jsonActual2
                expected = jsonExpected1
            } )
            assert(res.breaksCount == 1)
            assert(res.getBreaks(0).equals(comparisonBreak {
                expectedValue = "Joe Bloggs"
                actualValue = "XXX"
                fieldName = "name"
                expectedBreak = false
            }))

        }
    }
    @Test
    fun personAddressMismatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = jsonActual3
                expected = jsonExpected1
            } )
            assert(res.breaksCount == 1)
            assert(res.getBreaks(0).fieldName=="address")

        }
    }
}