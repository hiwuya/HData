package me.jayer.hdata.hbase

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.hbase.transform.HBaseResultToRowFn
import me.jayer.hdata.hbase.transform.HBaseWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertNotNull

/**
 * The serialization boundary of the HBase write / result-conversion DoFns, and of the source produced by
 * the read provider's graph construction.
 *
 * These objects capture the codec / config / schema and must be serializable before the job is even
 * submitted; a unit test that only calls `processElement` would never catch a non-serializable field
 * hiding inside one.
 *
 * @author wuya
 */
class HBaseSerializationTest {

    @Test
    fun `the write DoFn is serializable`() {
        val config = HBaseWriteConfig(
            zookeeperQuorum = "localhost:2181", table = "t", schemaFields = listOf("name:STRING")
        )
        val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING"), "cf")
        val errorSchema = Schema.builder().addNullableStringField("document").build()
        SerializableUtils.ensureSerializable(HBaseWriteFn(config, codec, errorSchema, false, "WriteToHBase"))
    }

    @Test
    fun `the result-to-row DoFn is serializable`() {
        val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING"), "cf")
        SerializableUtils.ensureSerializable(HBaseResultToRowFn(codec))
    }

    @Test
    fun `the source produced by the read provider's graph construction is serializable`() {
        val transform = HBaseReadProvider().from(
            TransformConfig(
                "ReadFromHBase",
                SpecMappers.CONFIG.readTree(
                    """{"zookeeper_quorum": "localhost:2181", "table": "t", "schema_fields": ["name:STRING"]}"""
                ) as ObjectNode,
            )
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
