package me.jayer.hdata.jdbc.util

import me.jayer.hdata.jdbc.handler.ResultSetHandler
import me.jayer.hdata.jdbc.handler.ResultSetMetaDataHandler
import java.sql.Connection
import javax.sql.DataSource

/**
 * @author wuya
 * @date 2022-08-11
 */
object SqlRunner {

    fun <T> query(dataSource: DataSource, sql: String, handler: me.jayer.hdata.jdbc.handler.ResultSetHandler<T>, vararg args: Any): T {
        return dataSource.connection.use { query(it, sql, handler, args) }
    }

    fun <T> query(connection: Connection, sql: String, handler: me.jayer.hdata.jdbc.handler.ResultSetHandler<T>, vararg args: Any): T {
        return connection.prepareStatement(sql).use { ps ->
            for ((index, value) in args.withIndex()) {
                ps.setObject(index + 1, value)
            }
            ps.executeQuery().use { rs ->
                handler.handle(rs)
            }
        }
    }

    fun <T> query(dataSource: DataSource, sql: String, handler: me.jayer.hdata.jdbc.handler.ResultSetMetaDataHandler<T>, vararg args: Any): T {
        return dataSource.connection.use { query(it, sql, handler, args) }
    }

    fun <T> query(connection: Connection, sql: String, handler: me.jayer.hdata.jdbc.handler.ResultSetMetaDataHandler<T>, vararg args: Any): T {
        return connection.prepareStatement(sql).use { ps ->
            for ((index, value) in args.withIndex()) {
                ps.setObject(index + 1, value)
            }
            handler.handle(ps.metaData)
        }
    }
}