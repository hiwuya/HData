package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.jayer.hdata.core.StructuredSource
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-08-05
 */
class JdbcStructuredSource(private val sourceDescriptor: JdbcSourceDescriptor) : StructuredSource() {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    override fun expand(input: PBegin): PCollection<Row> {
        val schema = HikariDataSource(HikariConfig(sourceDescriptor.dataSourceConfig)).use { dataSource ->
            val query = sourceDescriptor.query.ifBlank {
                var sql =
                    "SELECT ${sourceDescriptor.columns.joinToString(",")} FROM `${sourceDescriptor.table}`"
                if (sourceDescriptor.where.isNotBlank()) {
                    sql += " WHERE ${sourceDescriptor.where}"
                }
                sql += " LIMIT 1"
                sql
            }

            dataSource.connection.use { connection ->
                JdbcUtils.convertToBeamSchema(connection.prepareStatement(query).executeQuery().metaData)
            }
        }

        return input.apply(Create.of(null as Void?))
            .apply("", ParDo.of(JdbcSplittableDoFn(sourceDescriptor))).setRowSchema(schema)
    }
}