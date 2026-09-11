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
 * [BeamSchemaTransformAdapter] plugs Beam's native [SchemaTransformProvider]s found on the classpath into HData.
 *
 * This is the bridge that lets "off-the-shelf IOs from the Beam ecosystem (e.g. jdbc_read) be written directly into a pipeline file". The bridge's core obligation:
 * convert HData's config syntax tree into a config Row according to the `configurationSchema()` the provider declares, then hand it down,
 * and this conversion goes through [me.jayer.hdata.core.util.RowConverters.toRow] (which enables unknown-field / type validation),
 * so a config with a misspelled field or wrong type should error out, rather than being silently ignored by the native provider.
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
    fun `config is converted to a Row per the native provider's configurationSchema and handed down`() {
        val provider = FakeBeamProvider()
        val adapter = BeamSchemaTransformAdapter(provider)

        val result = adapter.from(cfg("""{"n": 5}"""))

        assertNotNull(result)
        assertEquals(5, provider.lastRow?.getInt32("n"))
    }

    @Test
    fun `identifier and ports are delegated directly to the native provider`() {
        val adapter = BeamSchemaTransformAdapter(FakeBeamProvider())
        assertEquals("beam:schematransform:fake:v1", adapter.identifier())
        assertEquals(listOf("Input"), adapter.inputCollectionNames())
        assertEquals(listOf("Output"), adapter.outputCollectionNames())
    }

    @Test
    fun `unknown fields in the config are rejected rather than ignored by the native provider`() {
        val adapter = BeamSchemaTransformAdapter(FakeBeamProvider())
        val error = assertFailsWith<HDataException> { adapter.from(cfg("""{"n": 5, "typo": 1}""")) }
        assertTrue("typo" in error.message!!, error.message)
    }

    @Test
    fun `a config type mismatch is rejected`() {
        val adapter = BeamSchemaTransformAdapter(FakeBeamProvider())
        val error = assertFailsWith<HDataException> { adapter.from(cfg("""{"n": "x"}""")) }
        assertTrue("expects a number" in error.message!!, error.message)
    }
}
