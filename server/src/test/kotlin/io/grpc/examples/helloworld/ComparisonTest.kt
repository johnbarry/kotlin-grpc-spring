package io.grpc.examples.helloworld

import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.json.XML

class ComparisonTest {
    private val server = ComparisonService()

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
    private val protoActual1 = xmlToProto(xmlActual1)
    private val protoActual2 = protoActual1.toBuilder()
        .setName("XXX")
        .build()
    private val protoActual3 = protoActual1.toBuilder()
        .addAddress("extra address line")
        .build()
    private fun xmlToProto(xml: String): Person2 =
        with (Person2.newBuilder()) {
            JsonFormat.parser().merge(XML.toJSONObject(xml).toString(2), this)
            build()
        }

    @Test
    fun personMatch() {
        runBlocking {
            val res = server.comparePerson( personComparison {
                actual = protoActual1
                expected = jsonExpected1
                identifier = jsonExpected1.id.toString()
            } )
            assert(res.unexpectedBreaksCount == 0)
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
        }
    }
}