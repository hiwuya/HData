package me.jayer.hdata.core.config

import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions

/**
 * @author wuya
 * @date 2022-08-30
 */
interface HDataOptions : PipelineOptions {

    @Description("Pipeline file path (.yaml / .yml)")
    fun getPipeline(): String?

    fun setPipeline(pipeline: String?)

    @Description("Only build and print the DAG without submitting it to run")
    @Default.Boolean(false)
    fun getDryRun(): Boolean

    fun setDryRun(dryRun: Boolean)

    @Description("Whether to wait for the job to finish; can be set to false for streaming jobs")
    @Default.Boolean(true)
    fun getWaitUntilFinish(): Boolean

    fun setWaitUntilFinish(waitUntilFinish: Boolean)

    @Description("Write a redacted JSON record at validation, submission, and terminal completion")
    fun getRunManifest(): String?

    fun setRunManifest(runManifest: String?)

    @Description("Comma-separated connector plugin directories; each directory contains one descriptor JAR and its private dependencies")
    fun getPluginDirectories(): String?

    fun setPluginDirectories(pluginDirectories: String?)
}
