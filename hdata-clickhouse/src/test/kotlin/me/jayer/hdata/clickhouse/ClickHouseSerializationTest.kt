package me.jayer.hdata.clickhouse

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.clickhouse.transform.ClickHouseReadFn
import me.jayer.hdata.clickhouse.transform.ClickHouseWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * A DoFn that captures a non-serializable object only blows up when the job is submitted; a unit
 * test that only calls `processElement` would never catch it, so every connector has at least one
 * `ensureSerializable` assertion.
 */
class ClickHouseSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            ClickHouseReadFn(ClickHouseReadConfig(query = "SELECT 1"))
        )
        SerializableUtils.ensureSerializable(
            ClickHouseWriteFn(
                ClickHouseWriteConfig(table = "test"),
                ErrorSchemas.of(Schema.builder().addStringField("col").build()),
                deadLetter = true,
                transformName = "WriteToClickHouse",
            )
        )
    }
}
