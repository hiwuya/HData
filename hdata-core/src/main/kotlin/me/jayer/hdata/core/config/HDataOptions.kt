package me.jayer.hdata.core.config

import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation.Required

/**
 * @author wuya
 * @date 2022-08-30
 */
interface HDataOptions : PipelineOptions {

    @Description("The path of config file")
    @Required
    fun getConfig(): String

    fun setConfig(config: String)
}