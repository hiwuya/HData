package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.filesystem.transform.FileRecordsFn
import me.jayer.hdata.filesystem.transform.TextLineToRowFn
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromFilesystem`：按 `path` 匹配文件并读出。
 *
 * 文件匹配复用 Beam 的 `FileIO.match()`（本身就是一个 Splittable DoFn），`text` 格式的读取
 * 复用 `TextIO.readFiles()`——它背后的 `ReadAllViaFileBasedSource` 按**字节区间**切分，
 * 单个大文件也能被多个 worker 分着读。
 *
 * 重构前这里有几个问题：
 *  - 自己写的 SDF 限制固定 `OffsetRange(0, 1)`，一个文件一个不可切分的处理单元，
 *    一个 10GB 的文件只能由一个 worker 从头读到尾；
 *  - 文件列表在构图阶段用 Hadoop `FileSystem.globStatus` 取，取完还 `fs.close()`——
 *    而 `FileSystem.get` 返回的是**进程内共享的缓存实例**，关掉它会波及同进程里
 *    所有正在用同一文件系统的代码；
 *  - CSV 用写死的 `CSVFormat.DEFAULT`，配置里的分隔符/引号设置根本没传下去。
 *
 * @author wuya
 */
class FilesystemReadProvider : TypedTransformProvider<FilesystemReadConfig>(FilesystemReadConfig::class.java) {

    override fun identifier(): String = "ReadFromFilesystem"

    override fun description(): String = "按 path 匹配文件并读出，复用 Beam 的 FileIO / TextIO"

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
        val schema = FilesystemSchemas.build(config)
        if (config.fileFormat == FilesystemReadConfig.TEXT) {
            return begin
                .apply("Match", FileIO.match().filepattern(FilesystemPaths.normalize(config.path)))
                .apply("ReadMatches", FileIO.readMatches())
                // 真正按字节区间切分的读取，单个大文件也能被多个 worker 分着读
                .apply("ReadLines", TextIO.readFiles())
                .apply("ToRow", ParDo.of(TextLineToRowFn()))
                .setRowSchema(schema)
        }
        return begin
            .apply("Match", FileIO.match().filepattern(FilesystemPaths.normalize(config.path)))
            .apply("ReadMatches", FileIO.readMatches())
            .apply("ReadRecords", ParDo.of(FileRecordsFn(config, schema)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
