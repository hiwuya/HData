package me.jayer.hdata.iceberg

import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.types.Types
import kotlin.test.Test
import kotlin.test.assertEquals

class IcebergMetricsTest {

    @Test
    fun `value count includes NULL, counted separately by null count`() {
        val schema = org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "age", Types.IntegerType.get()),
        )
        val nonNull = GenericRecord.create(schema).also { it.setField("age", 42) }
        val nullRow = GenericRecord.create(schema).also { it.setField("age", null) }

        val metrics = IcebergCatalogs.metricsFromRows(listOf(nonNull, nullRow), schema)

        assertEquals(2L, metrics.valueCounts()[1])
        assertEquals(1L, metrics.nullValueCounts()[1])
    }
}
