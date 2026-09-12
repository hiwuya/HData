package me.jayer.hdata.ftp

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.ftp.transform.FtpFile
import me.jayer.hdata.ftp.transform.FtpReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.commons.net.ftp.FTPFile
import org.slf4j.LoggerFactory
import java.nio.file.FileSystems
import java.nio.file.Path

/**
 * `ReadFromFtp`: lists remote files and reads them in parallel by byte range.
 *
 * The file list is taken at graph-construction time, so the machine that submits the job
 * must be able to reach the FTP server.
 *
 * @author wuya
 */
class FtpReadProvider : TypedTransformProvider<FtpReadConfig>(FtpReadConfig::class.java) {

    override fun identifier(): String = "ReadFromFtp"

    override fun description(): String = "List files under the FTP directory and read them in parallel by byte range, using a Splittable DoFn"

    override fun deliveryCapabilities(config: TransformConfig) = DeliveryCapabilities(
        DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.FULL_REPLAY, OrderingScope.NONE,
        notes = "File listings and byte ranges are recomputed for every job; a restart re-reads files and does not preserve row order across ranges.",
    )

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
        val schema = buildReadSchema(config)
        val files = listFiles()
        LOGGER.info("ReadFromFtp found {} files: {}", files.size, files.map { it.path })

        if (files.isEmpty()) {
            return begin.apply("Empty", Create.empty(schema)).setRowSchema(schema)
        }
        return begin.apply("Files", Create.of(files))
            .apply("Read", ParDo.of(FtpReadFn(config.connection, config)))
            .setRowSchema(schema)
    }

    /**
     * List the files to read.
     *
     * Before the refactor there was a subtle bug here: when the directory listed nothing it would
     * return `listOf(config.path)`, passing the **directory itself** down as a file to read, with an
     * error message that made no sense. Now we branch on the type FTP returns: a single file is read,
     * an empty directory returns an empty list.
     */
    private fun listFiles(): List<FtpFile> = withFtpClient(config.connection) { client ->
        val listed: Array<FTPFile> = client.listFiles(config.path) ?: emptyArray()

        // when path points to a single file, listFiles returns that file itself
        if (listed.size == 1 && listed[0].isFile && !config.path.endsWith("/")) {
            val single = listed[0]
            if (single.name == config.path.substringAfterLast('/')) {
                return@withFtpClient listOf(FtpFile(config.path, single.size))
            }
        }

        val matcher = config.filePattern?.let { FileSystems.getDefault().getPathMatcher("glob:$it") }
        listed.filter { it.isFile }
            .filter { matcher == null || matcher.matches(Path.of(it.name)) }
            .map { FtpFile(joinPath(config.path, it.name), it.size) }
    }

    private fun joinPath(dir: String, name: String): String =
        if (dir.endsWith("/")) "$dir$name" else "$dir/$name"

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(FtpReadProvider::class.java)
