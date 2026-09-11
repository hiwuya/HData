package me.jayer.hdata.iceberg.internal

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Metrics
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.Record
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.types.Conversions
import org.apache.iceberg.types.Type
import org.apache.iceberg.exceptions.CommitFailedException
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * Opens a local HadoopCatalog, loads / creates Iceberg tables on demand, and writes and commits a bundle's rows.
 *
 * @author wuya
 */
object IcebergCatalogs : Serializable {

    /**
     * `catalog_name` must actually be passed through: it appears in Iceberg's error messages, metrics, and table
     * identifiers. Using the two-argument constructor hardcodes the name to `hadoop`, meaning the config option is
     * accepted but then discarded.
     */
    fun openCatalog(warehouse: String, catalogName: String, hadoopConf: Map<String, String> = emptyMap()): HadoopCatalog {
        val catalog = HadoopCatalog()
        val conf = Configuration()
        hadoopConf.forEach { (k, v) -> conf.set(k, v) }
        catalog.setConf(conf)
        catalog.initialize(catalogName, mapOf("warehouse" to warehouse))
        return catalog
    }

    fun loadTable(catalog: Catalog, table: String): Table =
        catalog.loadTable(TableIdentifier.parse(table))

    fun ensureTable(catalog: Catalog, table: String, schema: org.apache.iceberg.Schema): Table {
        val id = TableIdentifier.parse(table)
        val resolved = if (catalog.tableExists(id)) catalog.loadTable(id) else try {
            catalog.createTable(id, schema)
        } catch (e: org.apache.iceberg.exceptions.AlreadyExistsException) {
            catalog.loadTable(id)
        }
        validateWritableTable(resolved, schema)
        return resolved
    }

    /** The writer currently supports only unpartitioned tables, and requires the declared fields to match the existing table by name, order, and type. */
    fun validateWritableTable(table: Table, expected: org.apache.iceberg.Schema) {
        require(table.spec().isUnpartitioned) {
            "Iceberg table [${table.name()}] is partitioned, and the current writer does not yet implement partitioned data files; use an unpartitioned table instead"
        }
        val actualFields = table.schema().columns()
        val expectedFields = expected.columns()
        require(actualFields.size == expectedFields.size) {
            "Iceberg table [${table.name()}] has ${actualFields.size} fields, but schema_fields declares ${expectedFields.size}"
        }
        actualFields.zip(expectedFields).forEach { (actual, declared) ->
            require(actual.name() == declared.name() && actual.type() == declared.type()) {
                "Iceberg table [${table.name()}] field [${actual.name()}:${actual.type()}] does not match " +
                    "schema_fields [${declared.name()}:${declared.type()}]"
            }
        }
    }

    /**
     * Clears the table's existing data; used by `write_mode: overwrite`.
     *
     * Uses `newDelete().deleteFromRowFilter(alwaysTrue())` rather than deleting files ourselves: Iceberg's delete is
     * a single atomic commit, so a reader sees either the old snapshot or the empty table, never a half-deleted
     * intermediate state. This step is **idempotent** — deleting again on an empty table does nothing, so bundle
     * retries are safe.
     */
    fun truncate(table: Table) {
        table.newDelete().deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue()).commit()
    }

    fun writeRecords(table: Table, rows: List<Record>, icebergSchema: org.apache.iceberg.Schema) {
        if (rows.isEmpty()) return
        val format = org.apache.iceberg.FileFormat.AVRO
        val location = table.location() + "/data/" + UUID.randomUUID().toString() + ".avro"
        val outputFile = table.io().newOutputFile(location)
        val appender: FileAppender<Record> = org.apache.iceberg.InternalData.write(format, outputFile)
            .schema(icebergSchema)
            .build()
        // Must be closed even on failure midway, otherwise the temp file's handle stays open; length()/metrics()
        // must be read after close.
        appender.use { rows.forEach(it::add) }
        // On Iceberg 1.10's AVRO appender, InternalData.write yields no column statistics (lower/upper_bounds are
        // empty), while aggregation/filter push-down relies on data file metadata, so here we compute our own
        // statistics from this bundle's rows and write them into the DataFile.
        val metrics = metricsFromRows(rows, icebergSchema)
        val dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
            .withPath(outputFile.location())
            .withFileSizeInBytes(appender.length())
            .withFormat(format)
            .withRecordCount(rows.size.toLong())
            .withMetrics(metrics)
            .build()
        appendWithRetry(table, dataFile)
    }

    /**
     * Multiple Beam bundles commit snapshots concurrently on different workers. Iceberg's own bounded optimistic
     * retries can still be exhausted when there are many concurrent bundles, after which HadoopCatalog fails with
     * `Version N already exists`. Here we retry only [CommitFailedException], which explicitly means "the commit did
     * not happen", refreshing the table first and adding randomized backoff each time; exceptions whose commit
     * status is unknown are never retried, otherwise the same data file could be appended twice.
     */
    private fun appendWithRetry(table: Table, dataFile: org.apache.iceberg.DataFile) {
        var conflicts = 0
        while (true) {
            try {
                table.refresh()
                table.newAppend().appendFile(dataFile).commit()
                return
            } catch (e: CommitFailedException) {
                if (conflicts >= MAX_COMMIT_CONFLICT_RETRIES) throw e
                val cap = minOf(MAX_COMMIT_BACKOFF_MILLIS, INITIAL_COMMIT_BACKOFF_MILLIS shl minOf(conflicts, 10))
                val delay = ThreadLocalRandom.current().nextLong(cap / 2 + 1, cap + 1)
                try {
                    Thread.sleep(delay)
                } catch (interrupted: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw IllegalStateException("Interrupted while waiting to retry a concurrent Iceberg commit", interrupted)
                }
                conflicts++
            }
        }
    }

    /**
     * Computes Iceberg column statistics ourselves from this bundle's rows. On 1.10's AVRO appender,
     * InternalData.write returns empty statistics, while aggregation/filter push-down relies on data file metadata,
     * so we fill in a set here: each column's min/max value is serialized per its Iceberg type into
     * lower/upper_bounds, and the null count goes into null_value_counts. Nested/non-scalar columns do not
     * participate in min/max (push-down aggregation does not support them either).
     */
    internal fun metricsFromRows(rows: List<Record>, schema: org.apache.iceberg.Schema): Metrics {
        val nullCounts = mutableMapOf<Int, Long>()
        val valueCounts = mutableMapOf<Int, Long>()
        val lower = mutableMapOf<Int, ByteBuffer>()
        val upper = mutableMapOf<Int, ByteBuffer>()
        schema.columns().forEach { field ->
            val id = field.fieldId()
            val type = field.type()
            var minV: Comparable<Any>? = null
            var maxV: Comparable<Any>? = null
            var nulls = 0L
            for (r in rows) {
                val v = r.getField(field.name())
                if (v == null) {
                    nulls++
                    continue
                }
                @Suppress("UNCHECKED_CAST")
                val c = (v as? Comparable<Any>) ?: return@forEach
                if (minV == null || minV.compareTo(c) > 0) minV = c
                if (maxV == null || maxV.compareTo(c) < 0) maxV = c
            }
            nullCounts[id] = nulls
            // Iceberg's value_counts is "the total number of values for this field in the file", including nulls;
            // null_value_counts separately describes the null count. Writing the non-null count instead would make
            // the inclusive metrics evaluator mistake "1 null + 1 non-null" for an all-null column, and prune away
            // files that still contain matching rows.
            valueCounts[id] = rows.size.toLong()
            if (minV != null) lower[id] = Conversions.toByteBuffer(type, minV)
            if (maxV != null) upper[id] = Conversions.toByteBuffer(type, maxV)
        }
        return Metrics(
            rows.size.toLong(),
            emptyMap(),
            valueCounts,
            nullCounts,
            emptyMap(),
            lower,
            upper,
        )
    }

    private const val MAX_COMMIT_CONFLICT_RETRIES = 20
    private const val INITIAL_COMMIT_BACKOFF_MILLIS = 25L
    private const val MAX_COMMIT_BACKOFF_MILLIS = 1_000L
}
