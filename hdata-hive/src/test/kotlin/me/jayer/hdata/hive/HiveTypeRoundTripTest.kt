package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TypeDescriptors
import tools.jackson.databind.node.ObjectNode
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.Test

/**
 * Whether each column type reads back as the value that was written.
 *
 * Time types are the focus: Hive's `timestamp` is a **wall-clock time without time zone**,
 * while ORC / Parquet / Avro all store a point in time underneath; if that one conversion picks the wrong time zone
 * the data shifts by several hours as a whole, and nothing reports an error along the way.
 *
 * @author wuya
 */
class HiveTypeRoundTripTest {

    /** Scalar types supported by every format. */
    private val scalarColumns = listOf(
        "c_bool" to "boolean",
        "c_tinyint" to "tinyint",
        "c_smallint" to "smallint",
        "c_int" to "int",
        "c_bigint" to "bigint",
        "c_float" to "float",
        "c_double" to "double",
        "c_string" to "string",
        "c_decimal" to "decimal(12,3)",
        "c_date" to "date",
        "c_ts" to "timestamp",
    )

    private val scalarSchema: Schema = Schema.builder()
        .addNullableField("c_bool", FieldTypes.BOOLEAN)
        .addNullableField("c_tinyint", FieldTypes.BYTE)
        .addNullableField("c_smallint", FieldTypes.INT16)
        .addNullableField("c_int", FieldTypes.INT32)
        .addNullableField("c_bigint", FieldTypes.INT64)
        .addNullableField("c_float", FieldTypes.FLOAT)
        .addNullableField("c_double", FieldTypes.DOUBLE)
        .addNullableField("c_string", FieldTypes.STRING)
        .addNullableField("c_decimal", FieldTypes.DECIMAL)
        .addNullableField("c_date", FieldTypes.DATE)
        .addNullableField("c_ts", FieldTypes.DATETIME)
        .build()

    private fun scalarRow(): Row = Row.withSchema(scalarSchema)
        .addValues(
            true,
            7.toByte(),
            300.toShort(),
            70_000,
            9_000_000_000L,
            1.5f,
            2.25,
            "中文 with space",
            BigDecimal("123.450"),
            LocalDate.of(2024, 2, 29),
            LocalDateTime.of(2024, 2, 29, 23, 59, 58),
        )
        .build()

    private val expectedText = listOf(
        "true", "7", "300", "70000", "9000000000", "1.5", "2.25",
        "中文 with space", "123.450", "2024-02-29", "2024-02-29T23:59:58",
    ).joinToString("|")

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private fun roundTrip(format: HiveStorageFormat, columns: List<Pair<String, String>>, schema: Schema, row: Row): String {
        TestHive().use { hive ->
            hive.createTable("t_types", format, columns)
            val writePipeline = Pipeline.create()
            val input = writePipeline.apply(Create.of(listOf(row)).withRowSchema(schema))
            HiveWriteProvider()
                .from(config("""
                    metastore_uri: "${hive.metastoreUri}"
                    table: t_types
                """.trimIndent()))
                .expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))
            writePipeline.run().waitUntilFinish()

            val readPipeline = Pipeline.create()
            val output = HiveReadProvider()
                .from(config("""
                    metastore_uri: "${hive.metastoreUri}"
                    table: t_types
                """.trimIndent()))
                .expand(PCollectionRowTuple.empty(readPipeline))
                .get(Tags.MAIN_OUTPUT)
            val text = output.apply(
                MapElements.into(TypeDescriptors.strings()).via(
                    object : SimpleFunction<Row, String>() {
                        override fun apply(input: Row): String = input.schema.fieldNames
                            .joinToString("|") { input.getValue<Any?>(it)?.toString() ?: "<null>" }
                    }
                )
            )
            PAssert.that(text).containsInAnyOrder(expectedText)
            readPipeline.run().waitUntilFinish()
            return expectedText
        }
    }

    @Test
    fun `标量类型在八种格式里都能原样往返`() {
        HiveStorageFormat.entries.forEach { format ->
            roundTrip(format, scalarColumns, scalarSchema, scalarRow())
        }
    }

    @Test
    fun `全 null 的行也能原样往返`() {
        val nulls = Row.withSchema(scalarSchema).apply { repeat(scalarSchema.fieldCount) { addValue(null) } }.build()
        HiveStorageFormat.entries.forEach { format ->
            TestHive().use { hive ->
                hive.createTable("t_nulls", format, scalarColumns)
                val writePipeline = Pipeline.create()
                val input = writePipeline.apply(Create.of(listOf(nulls)).withRowSchema(scalarSchema))
                HiveWriteProvider()
                    .from(config("""
                        metastore_uri: "${hive.metastoreUri}"
                        table: t_nulls
                    """.trimIndent()))
                    .expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))
                writePipeline.run().waitUntilFinish()

                val readPipeline = Pipeline.create()
                val output = HiveReadProvider()
                    .from(config("""
                        metastore_uri: "${hive.metastoreUri}"
                        table: t_nulls
                    """.trimIndent()))
                    .expand(PCollectionRowTuple.empty(readPipeline))
                    .get(Tags.MAIN_OUTPUT)
                val text = output.apply(
                    MapElements.into(TypeDescriptors.strings()).via(
                        object : SimpleFunction<Row, String>() {
                            override fun apply(input: Row): String = input.schema.fieldNames
                                .joinToString("|") { input.getValue<Any?>(it)?.toString() ?: "<null>" }
                        }
                    )
                )
                PAssert.that(text).containsInAnyOrder(List(scalarSchema.fieldCount) { "<null>" }.joinToString("|"))
                readPipeline.run().waitUntilFinish()
            }
        }
    }
}
