package me.jayer.hdata.core.spi

/**
 * @author wuya
 * @date 2022-07-22
 */
interface StructuredTransformProvider {

    fun identifier(): String

    fun createTransform(config: Map<String, Any>): StructuredTransform
}