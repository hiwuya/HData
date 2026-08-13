package me.jayer.hdata.jdbc.handler

import java.sql.ResultSet

/**
 * @author wuya
 * @date 2022-08-11
 */
abstract class AbstractListResultSetHandler<T> : ResultSetHandler<List<T>> {

    abstract fun handleRow(rs: ResultSet): T

    override fun handle(rs: ResultSet): List<T> {
        val list = mutableListOf<T>()
        while (rs.next()) {
            list.add(handleRow(rs))
        }
        return list
    }
}