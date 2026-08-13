package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.filesystem.transform.FilesystemReadFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.net.URI

private val LOGGER = LoggerFactory.getLogger(FilesystemReadProvider::class.java)

/**
 * `ReadFromFilesystem`：按 `path` 通配/目录列出文件，每个文件一个 element，用 Splittable DoFn 并行读。
 */
class FilesystemReadProvider : TypedTransformProvider<FilesystemReadConfig>(FilesystemReadConfig::class.java) {

    override fun identifier(): String = "ReadFromFilesystem"

    override fun description(): String = "按 path 通配/目录列出文件，用 Splittable DoFn 并行读取"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: FilesystemReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return FilesystemSource(config)
    }
}

private class FilesystemSource(private val config: FilesystemReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val files = listFiles()
        LOGGER.info("ReadFromFilesystem 共 {} 个文件: {}", files.size, files)
        val schema = FilesystemSchemas.build(config)
        return begin.apply("Files", Create.of(files))
            .apply("Read", ParDo.of(FilesystemReadFn(config, schema)))
            .setRowSchema(schema)
    }

    private fun listFiles(): List<String> {
        val conf = Configuration().apply { this["fs.defaultFS"] = config.defaultFs }
        val fs = FileSystem.get(URI(config.defaultFs), conf)
        val path = Path(config.path)
        val matched = fs.globStatus(path)
        val statuses = if (matched != null && matched.isNotEmpty()) matched else fs.listStatus(path)
        return statuses.filter { it.isFile }.map { it.path.toString() }.also {
            fs.close()
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
