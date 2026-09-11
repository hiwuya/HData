package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.transform.IcebergReadFileFn
import me.jayer.hdata.iceberg.transform.IcebergLimitedReadFn
import me.jayer.hdata.iceberg.transform.IcebergSplitEnumeratorFn
import me.jayer.hdata.iceberg.transform.IcebergTruncateFn
import me.jayer.hdata.iceberg.transform.IcebergWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import kotlin.test.Test

class IcebergSerializationTest {

    @Test
    fun `DoFn 可序列化`() {
        val readConfig = IcebergReadConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"))
        val schema = readConfig.outputSchema()
        SerializableUtils.ensureSerializable(IcebergSplitEnumeratorFn(readConfig))
        SerializableUtils.ensureSerializable(IcebergReadFileFn(readConfig, schema, parseSchemaFields(readConfig.schemaFields)))
        SerializableUtils.ensureSerializable(
            IcebergLimitedReadFn(readConfig.copy(limit = 1), schema, parseSchemaFields(readConfig.schemaFields)),
        )

        val writeConfig = IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"))
        SerializableUtils.ensureSerializable(
            IcebergWriteFn(writeConfig, ErrorSchemas.of(schema), deadLetter = true, transformName = "WriteToIceberg"),
        )
        // The overwrite table-clear step is attached to the write ParDo via a side input, so it must be serializable too.
        SerializableUtils.ensureSerializable(IcebergTruncateFn(writeConfig))
    }
}
