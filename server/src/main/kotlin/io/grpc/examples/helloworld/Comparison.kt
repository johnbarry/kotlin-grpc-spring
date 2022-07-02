package io.grpc.examples.helloworld

class Comparison {
    class FieldComparison(val field: String) {
        var actual: String? = null
        var expected: String? = null
    }
    val result: ComparisonResult.Builder = ComparisonResult.newBuilder()

    fun addBreak(brk: ComparisonBreak) {
        result.addBreaks(brk)
    }

    fun compareValue(f: String, init: FieldComparison.() -> Unit): FieldComparison =
        FieldComparison(f).also { cmp: FieldComparison ->
            cmp.init()
            if (!cmp.actual.equals(cmp.expected))
                addBreak(comparisonBreak {
                    fieldName = cmp.field
                    expectedValue = cmp.expected ?: ""
                    actualValue = cmp.actual ?: ""
                })

        }
    fun comparing(f: String, cmp: FieldComparison.() -> Unit) {
        with (FieldComparison(f)) {
            cmp()
        }
    }

}


fun comparison(init: Comparison.() -> Unit): ComparisonResult =
    with (Comparison()) {
        init()
        result.build()
    }
