package io.jayer.hdata.core

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-07-22
 */
interface StructuredIOProvider {

    fun identifier(): String

    fun configSchema(): Schema

    fun createSource(config: Row): StructuredSource

    fun createSink(config: Row): StructuredSink
}