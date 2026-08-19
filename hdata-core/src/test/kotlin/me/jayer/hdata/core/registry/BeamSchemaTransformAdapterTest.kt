package me.jayer.hdata.core.registry

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.transforms.SchemaTransform
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * [BeamSchemaTransformAdapter] 把 classpath 上的 Beam 原生 [SchemaTransformProvider] 接进 HData。
 *
 * 这是「Beam 生态现成的 IO（如 jdbc_read）能直接写进 pipeline 文件」的桥。桥的核心义务：
 * 把 HData 的配置语法树按 provider 自己声明的 `configurationSchema()` 转成配置 Row 再下发，
 * 而且这个转换走 [me.jayer.hdata.core.util.RowConverters.toRow]（开启未知字段/类型校验），
 * 因此配置写错字段或类型时应当报错，而不是被原生 provider 静默忽略。
 */
class BeamSchemaTransformAdapterTest {

    private class FakeBeamProvider : SchemaTransformProvider {
        var lastRow: Row? = null

        override fun identifier(): String = "beam:schematransform:fake:v1"
        override fun inputCollectionNames(): List<String> = listOf("Input")
        override fun outputCollectionNames(): List<String> = listOf("Output")
        override fun configurationSchema(): Schema = Schema.builder().addInt32Field("n").build()

        override fun from(configuration: Row): SchemaTransform {
            lastRow = configuration
            return object : SchemaTransform() {
                override fun expand(input: PCollectionRowTuple): PCollectionRowTuple = input
            }
        }
    }

    private fun cfg(text: String): TransformConfig =
        TransformConfig("t", SpecMappers.CONFIG.readTree(text) as ObjectNode)

    @Test
    fun `配置按原生 provider 的 configurationSchema 转换成 Row 并下发`() {
        val provider = FakeBeamProvider()
        val adapter = BeamSchemaTransformAdapter(provider)

        val result = adapter.from(cfg("""{"n": 5}"""))

        assertNotNull(result)
        assertEquals(5, provider.lastRow?.getInt32("n"))
    }

    @Test
    fun `标识符与端口直接委派给原生 provider`() {
        val adapter = BeamSchemaTransformAdapter(FakeBeamProvider())
        assertEquals("beam:schematransform:fake:v1", adapter.identifier())
        assertEquals(listOf("Input"), adapter.inputCollectionNames())
        assertEquals(listOf("Output"), adapter.outputCollectionNames())
    }

    @Test
    fun `配置里的未知字段会被拒绝而不是被原生 provider 忽略`() {
        val adapter = BeamSchemaTransformAdapter(FakeBeamProvider())
        val error = assertFailsWith<HDataException> { adapter.from(cfg("""{"n": 5, "typo": 1}""")) }
        assertTrue("typo" in error.message!!, error.message)
    }

    @Test
    fun `配置类型不匹配会被拒绝`() {
        val adapter = BeamSchemaTransformAdapter(FakeBeamProvider())
        val error = assertFailsWith<HDataException> { adapter.from(cfg("""{"n": "x"}""")) }
        assertTrue("期望是数字" in error.message!!, error.message)
    }
}
