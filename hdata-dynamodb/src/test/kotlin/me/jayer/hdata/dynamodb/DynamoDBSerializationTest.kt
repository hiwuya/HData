package me.jayer.hdata.dynamodb

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.dynamodb.transform.DynamoDBReadFn
import me.jayer.hdata.dynamodb.transform.DynamoDBWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * A DoFn that captures a non-serializable object only blows up when the job is submitted; a unit
 * test that only calls `processElement` would never catch it, so every connector has at least one
 * `ensureSerializable` assertion.
 */
class DynamoDBSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            DynamoDBReadFn(DynamoDBReadConfig(tableName = "test-table"))
        )
        SerializableUtils.ensureSerializable(
            DynamoDBWriteFn(
                DynamoDBWriteConfig(tableName = "test-table"),
                ErrorSchemas.of(Schema.builder().addStringField("col").build()),
                deadLetter = true,
                transformName = "WriteToDynamoDB",
            )
        )
    }
}
