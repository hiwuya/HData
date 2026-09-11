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
 * FTP's DoFn / connection params / provider transform must be serializable, otherwise submitting
 * to a distributed runner would blow up.
 *
 * @author wuya
 */
class FtpSerializationTest {

    private val connection = FtpConnection(host = "127.0.0.1", port = 21, user = "u", password = "p")
    private val readConfig = FtpReadConfig(host = "127.0.0.1", port = 21, user = "u", password = "p", path = "/in")
    private val writeConfig = FtpWriteConfig(host = "127.0.0.1", path = "/out")
    private val errorSchema = ErrorSchemas.of(Schema.builder().addNullableStringField("content").build())

    @Test
    fun `FtpReadFn is serializable`() {
        SerializableUtils.ensureSerializable(FtpReadFn(connection, readConfig))
    }

    @Test
    fun `FtpWriteFn is serializable`() {
        SerializableUtils.ensureSerializable(FtpWriteFn(writeConfig, errorSchema, true, "WriteToFtp"))
    }

    @Test
    fun `the transform produced by the read provider is serializable`() {
        val transform = FtpReadProvider().from(
            TransformConfig("ReadFromFtp", SpecMappers.YAML.readTree("host: h\npath: /in") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `the transform produced by the write provider is serializable`() {
        val transform = FtpWriteProvider().from(
            TransformConfig("WriteToFtp", SpecMappers.YAML.readTree("host: h\npath: /out") as ObjectNode)
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
