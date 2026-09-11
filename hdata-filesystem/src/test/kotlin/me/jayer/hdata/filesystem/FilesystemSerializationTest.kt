package me.jayer.hdata.filesystem

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.filesystem.transform.FileRecordsFn
import me.jayer.hdata.filesystem.transform.RowToLineFn
import me.jayer.hdata.filesystem.transform.TextLineToRowFn
import me.jayer.hdata.filesystem.transform.XlsxSink
import me.jayer.hdata.filesystem.transform.EncodedTextSink
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.TupleTag
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertNotNull

/**
 * Serialization boundaries of the Filesystem DoFns / Sinks / provider transforms.
 *
 * The write path is handed to Beam's `FileIO.write()`, so `FileRecordsFn` / `RowToLineFn` /
 * `XlsxSink` are serialized and shipped along with the transform -- capturing a non-serializable
 * object only blows up when the job is submitted.
 *
 * @author wuya
 */
class FilesystemSerializationTest {

    private val schema: Schema = Schema.builder()
        .addNullableStringField("name")
        .addNullableInt32Field("age")
        .build()
    private val readConfig = FilesystemReadConfig(path = "/in", fileFormat = "csv", schemaFields = listOf("name:string", "age:int"))
    private val writeConfig = FilesystemWriteConfig(path = "/out", fileFormat = "csv", schemaFields = listOf("name:string", "age:int"))
    private val errorSchema = ErrorSchemas.of(schema)

    @Test
    fun `FileRecordsFn is serializable`() = SerializableUtils.ensureSerializable(FileRecordsFn(readConfig, schema))

    @Test
    fun `RowToLineFn is serializable`() =
        SerializableUtils.ensureSerializable(RowToLineFn(writeConfig, errorSchema, true, "w", TupleTag("e")))

    @Test
    fun `XlsxSink is serializable`() = SerializableUtils.ensureSerializable(XlsxSink(writeConfig, schema))

    @Test
    fun `EncodedTextSink is serializable`() = SerializableUtils.ensureSerializable(EncodedTextSink("GB18030", "name,age"))

    @Test
    fun `TextLineToRowFn is serializable`() = SerializableUtils.ensureSerializable(TextLineToRowFn())

    @Test
    fun `the read provider transform is serializable`() {
        val transform = FilesystemReadProvider().from(
            TransformConfig("ReadFromFilesystem", SpecMappers.YAML.readTree("path: /in\nfile_format: csv\nschema_fields: [\"name:string\"]") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `the write provider transform is serializable`() {
        val transform = FilesystemWriteProvider().from(
            TransformConfig("WriteToFilesystem", SpecMappers.YAML.readTree("path: /out\nfile_format: csv\nschema_fields: [\"name:string\"]") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
