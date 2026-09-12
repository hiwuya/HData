package me.jayer.hdata.core

import org.apache.beam.sdk.options.FileStagingOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import kotlin.test.Test
import kotlin.test.assertEquals

class PluginArtifactStagingTest {

    @Test
    fun `plugin artifacts are merged into Beam files to stage without duplicates`() {
        val options = PipelineOptionsFactory.create()
        options.`as`(FileStagingOptions::class.java).filesToStage = listOf("host.jar", "plugin.jar")

        HData.stagePluginArtifacts(options, listOf("plugin.jar", "client.jar"))

        assertEquals(
            listOf("host.jar", "plugin.jar", "client.jar"),
            options.`as`(FileStagingOptions::class.java).filesToStage,
        )
    }
}
