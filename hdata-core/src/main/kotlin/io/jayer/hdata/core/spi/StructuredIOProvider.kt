package io.jayer.hdata.core.spi

/**
 * @author wuya
 * @date 2022-07-22
 */
interface StructuredIOProvider {

    fun identifier(): String

    fun createSource(config: Map<String, Any>): StructuredSource

    fun createSink(config: Map<String, Any>): StructuredSink
}