package io.grpc.examples.helloworld

class CompareRecord {
    class Check() {
        var actual: String? = null
        var expected: String? = null
        fun checkField(init: Check.() -> Unit): Check =
            Check().apply {
                init()
            }

    }
}


fun compareRecord(init: CompareRecord.() -> Unit): CompareRecord =
    CompareRecord().apply {
        init()
    }
