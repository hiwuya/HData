package me.jayer.hdata.ftp

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.ftp.transform.FtpReadFn
import me.jayer.hdata.ftp.transform.FtpWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertNotNull

/**
 * FTP 的 DoFn / 连接参数 / provider transform 必须可序列化，否则提交到分布式 runner 时会炸。
 *
 * @author wuya
 */
class FtpSerializationTest {

    private val connection = FtpConnection(host = "127.0.0.1", port = 21, user = "u", password = "p")
    private val readConfig = FtpReadConfig(host = "127.0.0.1", port = 21, user = "u", password = "p", path = "/in")
    private val writeConfig = FtpWriteConfig(host = "127.0.0.1", path = "/out")
    private val errorSchema = ErrorSchemas.of(Schema.builder().addNullableStringField("content").build())

    @Test
    fun `FtpReadFn 可序列化`() {
        SerializableUtils.ensureSerializable(FtpReadFn(connection, readConfig))
    }

    @Test
    fun `FtpWriteFn 可序列化`() {
        SerializableUtils.ensureSerializable(FtpWriteFn(writeConfig, errorSchema, true, "WriteToFtp"))
    }

    @Test
    fun `读取端 provider 生成的 transform 可序列化`() {
        val transform = FtpReadProvider().from(
            TransformConfig("ReadFromFtp", SpecMappers.YAML.readTree("host: h\npath: /in") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `写入端 provider 生成的 transform 可序列化`() {
        val transform = FtpWriteProvider().from(
            TransformConfig("WriteToFtp", SpecMappers.YAML.readTree("host: h\npath: /out") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
