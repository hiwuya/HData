package io.jayer.hdata.jdbc

import io.jayer.hdata.core.StructuredIOProvider
import io.jayer.hdata.core.StructuredSink
import io.jayer.hdata.core.StructuredSource
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-08-05
 */
class JdbcStructuredIOProvider: StructuredIOProvider {

    override fun identifier(): String = "jdbc"

    override fun configSchema(): Schema {
        TODO("Not yet implemented")
    }

    override fun createSource(config: Row): StructuredSource {
        TODO("Not yet implemented")
    }

    override fun createSink(config: Row): StructuredSink {
        TODO("Not yet implemented")
    }
}