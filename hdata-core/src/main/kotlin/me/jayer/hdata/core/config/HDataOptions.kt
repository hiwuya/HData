package me.jayer.hdata.core.config

import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions

/**
 * @author wuya
 * @date 2022-08-30
 */
interface HDataOptions : PipelineOptions {

    @Description("pipeline 文件路径（.yaml / .yml）")
    fun getPipeline(): String?

    fun setPipeline(pipeline: String?)

    @Description("只构图并打印 DAG，不提交运行")
    @Default.Boolean(false)
    fun getDryRun(): Boolean

    fun setDryRun(dryRun: Boolean)

    @Description("是否等待作业结束，流式作业可设为 false")
    @Default.Boolean(true)
    fun getWaitUntilFinish(): Boolean

    fun setWaitUntilFinish(waitUntilFinish: Boolean)
}
