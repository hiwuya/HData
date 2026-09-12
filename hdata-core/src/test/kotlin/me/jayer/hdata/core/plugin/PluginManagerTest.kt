package me.jayer.hdata.core.plugin

import me.jayer.hdata.core.spi.PluginDescriptor
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.registry.TransformRegistry
import me.jayer.hdata.testplugin.ExternalPluginProvider
import me.jayer.hdata.testplugin.ExternalPluginSource
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.JsonNodeFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import kotlin.io.path.outputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertTrue

class PluginManagerTest {

    @Test
    fun `isolated plugin provider is discovered and executes on DirectRunner`() {
        val directory = Files.createTempDirectory("hdata-plugin-")
        createPluginJar(directory)

        TransformRegistry.discover(pluginDirectories = listOf(directory)).use { registry ->
            val provider = registry.get("ExternalPlugin")
            assertNotSame(ExternalPluginProvider::class.java.classLoader, provider.javaClass.classLoader)
            assertEquals(listOf(directory.resolve("external-plugin.jar").toString()), registry.pluginArtifacts)

            val pipeline = Pipeline.create()
            val output = provider.from(TransformConfig("external", JsonNodeFactory.instance.objectNode()))
                .expand(PCollectionRowTuple.empty(pipeline))
                .get(Tags.MAIN_OUTPUT)
            val expected = Row.withSchema(output.schema).addValue("isolated-plugin").build()
            PAssert.that(output).containsInAnyOrder(expected)
            assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish())
        }
    }

    @Test
    fun `plugin descriptor rejects an incompatible API version`() {
        val directory = Files.createTempDirectory("hdata-plugin-")
        createPluginJar(directory, apiVersion = "999")
        val error = kotlin.test.assertFailsWith<me.jayer.hdata.core.exception.HDataException> {
            TransformRegistry.discover(pluginDirectories = listOf(directory))
        }
        assertTrue(error.message!!.contains("requires API 999"))
    }

    private fun createPluginJar(directory: Path, apiVersion: String = PluginDescriptor.API_VERSION) {
        JarOutputStream(directory.resolve("external-plugin.jar").outputStream()).use { jar ->
            listOf(ExternalPluginProvider::class.java, ExternalPluginSource::class.java).forEach { type ->
                val resource = type.name.replace('.', '/') + ".class"
                jar.putNextEntry(JarEntry(resource))
                type.classLoader.getResourceAsStream(resource)!!.use { it.copyTo(jar) }
                jar.closeEntry()
            }
            jar.putNextEntry(JarEntry(PluginDescriptor.RESOURCE_PATH))
            jar.write(
                """
                id=external-test
                api_version=$apiVersion
                provider_class=${ExternalPluginProvider::class.java.name}
                """.trimIndent().toByteArray(),
            )
            jar.closeEntry()
        }
    }
}
