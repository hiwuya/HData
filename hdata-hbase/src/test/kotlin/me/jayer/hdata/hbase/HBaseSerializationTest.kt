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
 * HBase 写入 / 结果转换 DoFn，以及读取 provider 构图生成的 source 的序列化边界。
 *
 * 这些对象捕获了 codec / 配置 / schema，必须在提交作业前就能序列化下发；
 * 一旦里面夹了不可序列化的东西，单测只调 `processElement` 永远发现不了。
 *
 * @author wuya
 */
class HBaseSerializationTest {

    @Test
    fun `写入 DoFn 可序列化下发`() {
        val config = HBaseWriteConfig(
            zookeeperQuorum = "localhost:2181", table = "t", schemaFields = listOf("name:STRING")
        )
        val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING"), "cf")
        val errorSchema = Schema.builder().addNullableStringField("document").build()
        SerializableUtils.ensureSerializable(HBaseWriteFn(config, codec, errorSchema, false, "WriteToHBase"))
    }

    @Test
    fun `结果转行 DoFn 可序列化下发`() {
        val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING"), "cf")
        SerializableUtils.ensureSerializable(HBaseResultToRowFn(codec))
    }

    @Test
    fun `读取 provider 生成的 source 可序列化下发`() {
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
