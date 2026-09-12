package me.jayer.hdata.clickhouse

import me.jayer.hdata.clickhouse.transform.ClickHouseReadSql
import kotlin.test.Test
import kotlin.test.assertEquals

class ClickHouseReadSqlTest {

    @Test
    fun `max rows wraps rather than appends to an already limited query`() {
        assertEquals(
            "SELECT * FROM (SELECT * FROM events LIMIT 20) AS _hdata_limited_read LIMIT 3",
            ClickHouseReadSql.withMaxRows("SELECT * FROM events LIMIT 20;", 3),
        )
    }
}
