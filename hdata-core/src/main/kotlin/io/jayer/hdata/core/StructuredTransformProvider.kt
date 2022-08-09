package io.jayer.hdata.core

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-07-22
 */
interface StructuredTransformProvider {

    fun identifier(): String

    fun configSchema(): Schema

    fun createTransform(config: Row): StructuredTransform
}