package me.jayer.hdata.core.registry

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.Tags
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class TransformRegistryTest {

    private val registry = TransformRegistry.discover()

    @Test
    fun `all built-in transforms can be discovered`() {
        val builtin = listOf(
            "Create", "MapToFields", "Flatten", "LogForTesting", "StripErrorMetadata", "AssertEqual",
        )
        builtin.forEach { assertNotNull(registry.find(it), "built-in transform not found: $it") }
    }

    @Test
    fun `providers in the test directory can also be discovered via ServiceLoader`() {
        assertNotNull(registry.find("TestSink"))
    }

    @Test
    fun `port declarations distinguish read side, processing side and write side`() {
        assertEquals(emptyList(), registry.get("Create").inputCollectionNames())
        assertEquals(listOf(Tags.MAIN_OUTPUT), registry.get("Create").outputCollectionNames())

        assertEquals(listOf(Tags.MAIN_INPUT), registry.get("MapToFields").inputCollectionNames())
        assertEquals(listOf(Tags.MAIN_OUTPUT), registry.get("MapToFields").outputCollectionNames())

        // Flatten is variadic
        assertEquals(listOf(Tags.ANY), registry.get("Flatten").inputCollectionNames())

        // the write side has no main output, only a dead-letter port
        assertEquals(listOf(Tags.ERROR_OUTPUT), registry.get("TestSink").outputCollectionNames())
    }

    @Test
    fun `find returns null for an unknown type, get throws`() {
        assertNull(registry.find("NoSuchTransform"))

        val error = assertFailsWith<HDataException> { registry.get("NoSuchTransform") }
        assertTrue("NoSuchTransform" in error.message!!)
        // the error should list the registered types, but should not dump the long list of Beam URNs
        assertTrue("Create" in error.message!!)
        assertFalse("beam:schematransform" in error.message!!)
    }

    @Test
    fun `a case mistake yields candidates`() {
        val error = assertFailsWith<HDataException> { registry.get("createx") }
        assertTrue("Create" in error.message!!)
    }

    @Test
    fun `a case-insensitive exact match is also suggested`() {
        val error = assertFailsWith<HDataException> { registry.get("create") }
        assertTrue("did you mean" in error.message!!)
        assertTrue("Create" in error.message!!)
    }

    @Test
    fun `a partial match (contains) yields candidates`() {
        val error = assertFailsWith<HDataException> { registry.get("creat") }
        assertTrue("did you mean" in error.message!!)
        assertTrue("Create" in error.message!!)
    }

    @Test
    fun `a completely unfamiliar type yields no candidates`() {
        val error = assertFailsWith<HDataException> { registry.get("zzz999") }
        assertFalse("did you mean" in error.message!!)
    }

    @Test
    fun `Beam's native SchemaTransformProviders on the classpath are registered by URN`() {
        // beam-sdks-java-core ships several SchemaTransformProviders; after bridging, their URNs should start with beam:
        val beamUrns = registry.identifiers.filter { it.startsWith("beam:") }
        beamUrns.forEach { urn ->
            assertTrue(registry.find(urn) is BeamSchemaTransformAdapter, "$urn should be provided by the adapter")
        }
    }

}
