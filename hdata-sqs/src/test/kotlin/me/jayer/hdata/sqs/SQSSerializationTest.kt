package me.jayer.hdata.sqs

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.sqs.transform.SQSReadFn
import me.jayer.hdata.sqs.transform.SQS_READ_SCHEMA
import me.jayer.hdata.sqs.transform.SQSWriteFn
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

class SQSSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            SQSReadFn(SQSReadConfig(queueUrl = "http://localhost:4566/queue/test"))
        )
        SerializableUtils.ensureSerializable(
            SQSWriteFn(
                SQSWriteConfig(queueUrl = "http://localhost:4566/queue/test"),
                ErrorSchemas.of(SQS_READ_SCHEMA),
                deadLetter = true,
                transformName = "WriteToSQS",
            )
        )
    }
}
