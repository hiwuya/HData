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
 * Filesystem 各 DoFn / Sink / provider transform 的序列化边界。
 *
 * 写路径交给了 Beam 的 `FileIO.write()`，所以 `FileRecordsFn` / `RowToLineFn` / `XlsxSink`
 * 会跟着 transform 一起序列化下发——捕获了不可序列化的对象只会在提交作业时才炸。
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
    fun `FileRecordsFn 可序列化`() = SerializableUtils.ensureSerializable(FileRecordsFn(readConfig, schema))

    @Test
    fun `RowToLineFn 可序列化`() =
        SerializableUtils.ensureSerializable(RowToLineFn(writeConfig, errorSchema, true, "w", TupleTag("e")))

    @Test
    fun `XlsxSink 可序列化`() = SerializableUtils.ensureSerializable(XlsxSink(writeConfig, schema))

    @Test
    fun `EncodedTextSink 可序列化`() = SerializableUtils.ensureSerializable(EncodedTextSink("GB18030", "name,age"))

    @Test
    fun `TextLineToRowFn 可序列化`() = SerializableUtils.ensureSerializable(TextLineToRowFn())

    @Test
    fun `读取端 provider transform 可序列化`() {
        val transform = FilesystemReadProvider().from(
            TransformConfig("ReadFromFilesystem", SpecMappers.YAML.readTree("path: /in\nfile_format: csv\nschema_fields: [\"name:string\"]") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `写入端 provider transform 可序列化`() {
        val transform = FilesystemWriteProvider().from(
            TransformConfig("WriteToFilesystem", SpecMappers.YAML.readTree("path: /out\nfile_format: csv\nschema_fields: [\"name:string\"]") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
