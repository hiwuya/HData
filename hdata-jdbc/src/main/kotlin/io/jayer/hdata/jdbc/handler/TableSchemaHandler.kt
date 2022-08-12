package io.jayer.hdata.jdbc.handler

import io.jayer.hdata.jdbc.Column
import java.sql.ResultSetMetaData

/**
 * @author wuya
 * @date 2022-08-12
 */
class TableSchemaHandler : AbstractListResultSetMetaDataHandler<Column>() {

    override fun handleRow(metaData: ResultSetMetaData, index: Int): Column = Column.from(metaData, index)
}