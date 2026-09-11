package me.jayer.hdata.cassandra

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.cassandra.transform.CassandraReadFn
import me.jayer.hdata.cassandra.transform.CassandraWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * A DoFn that captures a non-serializable object only blows up when the job is submitted; a unit
 * test that only calls `processElement` would never catch it, so every connector has at least one
 * `ensureSerializable` assertion.
 */
class CassandraSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            CassandraReadFn(CassandraReadConfig(keyspace = "ks", query = "SELECT 1"))
        )
        SerializableUtils.ensureSerializable(
            CassandraWriteFn(
                CassandraWriteConfig(keyspace = "ks", table = "t"),
                ErrorSchemas.of(Schema.builder().addStringField("col").build()),
                deadLetter = true,
                transformName = "WriteToCassandra",
            )
        )
    }
}
