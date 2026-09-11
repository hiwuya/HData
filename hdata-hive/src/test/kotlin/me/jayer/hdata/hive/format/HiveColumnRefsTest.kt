package me.jayer.hdata.hive.format

import org.apache.orc.TypeDescription
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.column.statistics.LongStatistics
import org.apache.beam.sdk.schemas.Schema
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals

class HiveColumnRefsTest {

    @Test
    fun `ORC top-level column ordinal must not be confused with the nested type tree's column id`() {
        val schema = TypeDescription.fromString("struct<nested:struct<x:int>,amount:int>")

        val ref = checkNotNull(orcTopLevelColumn(schema, "amount"))
        assertEquals(1, ref.ordinal)
        assertEquals(3, ref.id)
        assertEquals(TypeDescription.Category.INT, ref.type.category)
    }

    @Test
    fun `Parquet top-level column ordinal and leaf column ordinal are resolved separately`() {
        val schema = MessageTypeParser.parseMessageType(
            """
            message test {
              optional group nested { optional int32 x; }
              optional int32 amount;
            }
            """.trimIndent(),
        )

        val ref = checkNotNull(parquetTopLevelColumn(schema, "amount"))
        assertEquals(1, ref.ordinal)
        assertEquals(1, ref.leafOrdinal)
    }

    @Test
    fun `Parquet INT64 statistics do not lose precision by going through Double`() {
        @Suppress("DEPRECATION")
        val stats = LongStatistics().apply {
            updateStats(9_007_199_254_740_993L)
            updateStats(9_007_199_254_740_995L)
        }

        val (min, max) = parquetColumnRange(stats, Schema.FieldType.INT64, 0)
        assertEquals(BigDecimal("9007199254740993"), (min as NumericValue).v)
        assertEquals(BigDecimal("9007199254740995"), (max as NumericValue).v)
    }
}
