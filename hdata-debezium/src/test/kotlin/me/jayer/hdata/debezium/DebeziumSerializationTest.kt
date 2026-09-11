package me.jayer.hdata.debezium

import me.jayer.hdata.debezium.transform.DebeziumReadFn
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

class DebeziumSerializationTest {

    @Test
    fun `DoFn is serializable`() {
        val config = DebeziumReadConfig(connector = "mysql", host = "h", user = "u")
        SerializableUtils.ensureSerializable(DebeziumReadFn(config))
    }
}
