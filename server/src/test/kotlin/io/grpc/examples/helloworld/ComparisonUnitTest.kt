package io.grpc.examples.helloworld

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class ComparisonUnitTest {
    private val server = PersonComparisonService()

    private val jsonExpected1 = person {
        id = 50
        forename = "Joe"
        surname = "Bloggs"
        addressLine1 = "123 Main Street"
        city = "Cary"
    }
    private val xmlActual1 = """
        <id>50</id>
        <name>Joe Bloggs</name>
        <address>123 Main Street</address>
        <address>Cary</address>
    """.trimIndent()

    //private val protoActual1 = xmlToProto(xmlActual1)
    private val protoActual1 = Person2.newBuilder().apply {
        fromXML(xmlActual1)
    }.build()

    private val protoActual2 = protoActual1.toBuilder()
        .setName("XXX")
        .build()

    private val protoActual3 = protoActual1.toBuilder()
        .addAddress("extra line")
        .build()

    @Test
    fun personMatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = protoActual1
                expected = jsonExpected1
                identifier = jsonExpected1.id.toString()
            } )
            assert(res.result == ComparisonResultType.MATCHED)
        }
    }

    @Test
    fun personNameMismatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = protoActual2
                expected = jsonExpected1
            } )
            assert(res.unexpectedBreaksCount == 1)
            assert(res.getUnexpectedBreaks(0).equals(comparisonBreak {
                expectedValue = "Joe Bloggs"
                actualValue = "XXX"
                fieldName = "name"
            }))
            assert(res.result == ComparisonResultType.BREAKS)
        }
    }

    @Test
    fun personAddressMismatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = protoActual3
                expected = jsonExpected1
            } )
            assert(res.unexpectedBreaksCount == 1)
            assert(res.getUnexpectedBreaks(0).fieldName=="address")
            assert(res.result == ComparisonResultType.BREAKS)

        }
    }
}