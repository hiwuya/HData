package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.transform.IcebergReadFn
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
        SerializableUtils.ensureSerializable(IcebergReadFn(readConfig, schema, parseSchemaFields(readConfig.schemaFields)))

        val writeConfig = IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"))
        SerializableUtils.ensureSerializable(
            IcebergWriteFn(writeConfig, ErrorSchemas.of(schema), deadLetter = true, transformName = "WriteToIceberg"),
        )
        // overwrite 的清表步骤走 side input 挂在写入的 ParDo 上，它也得能序列化
        SerializableUtils.ensureSerializable(IcebergTruncateFn(writeConfig))
    }
}
