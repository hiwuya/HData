package io.jayer.hdata.core.config

import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation.Required

/**
 * @author wuya
 * @date 2022-08-30
 */
interface HDataOptions : PipelineOptions {

    @Description("Config file path")
    @Required
    fun getConfig(): String

    fun setConfig(config: String)
}