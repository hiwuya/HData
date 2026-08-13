package me.jayer.hdata.ftp

import me.jayer.hdata.ftp.transform.FtpReadFn
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.commons.net.ftp.FTPFile
import org.slf4j.LoggerFactory
import java.nio.file.FileSystems
import java.nio.file.Path

private val LOGGER = LoggerFactory.getLogger(FtpReadProvider::class.java)

/**
 * `ReadFromFtp`：列出 `path` 下匹配 `file_pattern` 的文件，用 Splittable DoFn 并行下载并逐行读出。
 */
class FtpReadProvider : TypedTransformProvider<FtpReadConfig>(FtpReadConfig::class.java) {

    override fun identifier(): String = "ReadFromFtp"

    override fun description(): String = "列出 FTP 目录下的文件并逐行读出，使用 Splittable DoFn 并行下载"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: FtpReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return FtpSource(config)
    }
}

private class FtpSource(private val config: FtpReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val files = listFiles()
        LOGGER.info("ReadFromFtp 共 {} 个文件: {}", files.size, files)
        val schema = buildReadSchema(config)
        return begin.apply("Files", Create.of(files))
            .apply(
                "Read",
                ParDo.of(
                    FtpReadFn(
                        config.connection,
                        config.fileFormat,
                        config.schemaFields,
                        config.encoding,
                        schema,
                    ),
                ),
            )
            .setRowSchema(schema)
    }

    private fun listFiles(): List<String> {
        val conn = config.connection
        val client = newFtpClient(conn)
        try {
            val pattern = config.filePattern
            val listed = client.listFiles(config.path)
            return if (pattern != null) {
                val matcher = FileSystems.getDefault().getPathMatcher("glob:$pattern")
                listed.filter { it.type == FTPFile.FILE_TYPE && matcher.matches(Path.of(it.name)) }
                    .map { joinPath(config.path, it.name) }
            } else if (listed.isNotEmpty()) {
                listed.filter { it.type == FTPFile.FILE_TYPE }.map { joinPath(config.path, it.name) }
            } else {
                listOf(config.path)
            }
        } finally {
            runCatching { client.logout() }
            runCatching { client.disconnect() }
        }
    }

    private fun joinPath(dir: String, name: String): String =
        if (dir.endsWith("/")) "$dir$name" else "$dir/$name"

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
