package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
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
 * `ReadFromFilesystem`: match files by `path` and read them out.
 *
 * File matching reuses Beam's `FileIO.match()` (itself a Splittable DoFn); the `text` format's read
 * reuses `TextIO.readFiles()` — the `ReadAllViaFileBasedSource` behind it splits by **byte range**, so
 * a single large file can be read by multiple workers.
 *
 * Before the refactor there were several problems here:
 *  - the hand-written SDF used a fixed restriction `OffsetRange(0, 1)`, one file as one unsplittable
 *    processing unit, so a 10GB file could only be read from start to end by a single worker;
 *  - the file list was taken at graph-construction time via Hadoop's `FileSystem.globStatus`, and then
 *    `fs.close()` was called — but `FileSystem.get` returns a **process-wide shared cached instance**,
 *    so closing it would affect all code in the same process using that filesystem;
 *  - CSV used a hard-coded `CSVFormat.DEFAULT`, so the delimiter/quote settings from the config were
 *    never passed through.
 *
 * @author wuya
 */
class FilesystemReadProvider : TypedTransformProvider<FilesystemReadConfig>(FilesystemReadConfig::class.java) {

    override fun identifier(): String = "ReadFromFilesystem"

    override fun description(): String = "Match files by path and read them out, reusing Beam's FileIO / TextIO"

    override fun inputCollectionNames(): List<String> = emptyList()

    // EXPERIMENTAL: S3FilesystemIT exercises this against a real S3-compatible store via Testcontainers
    // (local file:// paths are also covered by DirectRunner behavior tests), but there is no dead-letter
    // coverage and no runner-qualification test beyond DirectRunner yet.
    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities = DeliveryCapabilities(
        deliveryMode = DeliveryMode.AT_LEAST_ONCE,
        replayBehavior = ReplayBehavior.FULL_REPLAY,
        ordering = OrderingScope.NONE,
        notes = "Matches path at graph construction time and re-reads matched files in full on every run; " +
            "a full job restart re-reads everything again. Safe if the matched files are not modified between " +
            "runs. Byte-range splitting for large text files carries no cross-file row order.",
    )

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
        FilesystemHadoop.configure(begin.pipeline, config.defaultFs, config.hadoopConf)
        val schema = FilesystemSchemas.build(config)
        if (config.fileFormat == FilesystemReadConfig.TEXT) {
            return begin
                .apply("Match", FileIO.match().filepattern(FilesystemPaths.normalize(config.path, config.defaultFs)))
                .apply("ReadMatches", FileIO.readMatches())
                // the read that really splits by byte range, so a single large file can be read by multiple workers
                .apply("ReadLines", TextIO.readFiles())
                .apply("ToRow", ParDo.of(TextLineToRowFn()))
                .setRowSchema(schema)
        }
        return begin
            .apply("Match", FileIO.match().filepattern(FilesystemPaths.normalize(config.path, config.defaultFs)))
            .apply("ReadMatches", FileIO.readMatches())
            .apply("ReadRecords", ParDo.of(FileRecordsFn(config, schema)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
