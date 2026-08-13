package me.jayer.hdata.ftp

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
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
 * `ReadFromFtp`：列出远程文件并按字节区间并行读。
 *
 * 文件列表在构图阶段取，所以提交作业的机器需要能连上 FTP。
 *
 * @author wuya
 */
class FtpReadProvider : TypedTransformProvider<FtpReadConfig>(FtpReadConfig::class.java) {

    override fun identifier(): String = "ReadFromFtp"

    override fun description(): String = "列出 FTP 目录下的文件并按字节区间并行读，使用 Splittable DoFn"

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
        LOGGER.info("ReadFromFtp 共 {} 个文件: {}", files.size, files.map { it.path })

        if (files.isEmpty()) {
            return begin.apply("Empty", Create.empty(schema)).setRowSchema(schema)
        }
        return begin.apply("Files", Create.of(files))
            .apply("Read", ParDo.of(FtpReadFn(config.connection, config)))
            .setRowSchema(schema)
    }

    /**
     * 列出要读的文件。
     *
     * 重构前这里有个隐蔽的错处：目录列不出东西时会 `listOf(config.path)`，
     * 把**目录本身**当成一个文件交下去读，报错信息完全对不上号。现在按 FTP 返回的类型判断，
     * 是单个文件就读它，是空目录就返回空列表。
     */
    private fun listFiles(): List<FtpFile> = withFtpClient(config.connection) { client ->
        val listed: Array<FTPFile> = client.listFiles(config.path) ?: emptyArray()

        // path 指向单个文件时，listFiles 返回的就是它自己
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
