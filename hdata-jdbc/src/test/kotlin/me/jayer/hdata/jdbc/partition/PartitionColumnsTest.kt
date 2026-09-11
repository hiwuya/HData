package me.jayer.hdata.jdbc.partition

import me.jayer.hdata.jdbc.H2Database
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.SelectSql
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Selection and validation of the partition column.
 *
 * @author wuya
 * @date 2022-08-30
 */
class PartitionColumnsTest {

    private fun resolve(
        connection: Connection,
        table: String,
        requested: String = "",
        where: String = "",
    ): PartitionColumn? {
        val select = SelectSql(table, conditions = listOf(where))
        val columns = JdbcMetadata.describe(connection, select.render())
        return PartitionColumns.resolve(connection, table, columns, requested, select)
    }

    @Test
    fun `未指定时自动选中主键首列`() {
        H2Database.named("auto_pk").use { db ->
            db.execute("CREATE TABLE t_order (sub INT, id INT, PRIMARY KEY (sub, id))")

            db.useConnection { connection ->
                val column = resolve(connection, "t_order")
                // The one with KEY_SEQ=1 is SUB, not the one that comes first in declaration or dictionary order
                assertEquals("SUB", column?.name)
            }
        }
    }

    @Test
    fun `没有主键时退化为不分区并给出警告`() {
        H2Database.named("no_pk").use { db ->
            db.execute("CREATE TABLE t_order (id INT)")

            db.useConnection { connection ->
                assertNull(resolve(connection, "t_order"))
            }
        }
    }

    @Test
    fun `主键类型不支持分区时退化为不分区`() {
        H2Database.named("pk_unsupported").use { db ->
            db.execute("CREATE TABLE t_order (id VARCHAR(50) PRIMARY KEY)")

            db.useConnection { connection ->
                assertNull(resolve(connection, "t_order"))
            }
        }
    }

    @Test
    fun `显式指定时大小写不敏感，且返回库里实际的列名`() {
        H2Database.named("explicit_case").use { db ->
            db.execute("CREATE TABLE t_order (id INT NOT NULL)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order", requested = "id")?.name)
                assertEquals("ID", resolve(connection, "t_order", requested = "ID")?.name)
            }
        }
    }

    @Test
    fun `指定了不存在的列时报错并列出可选列`() {
        H2Database.named("explicit_missing").use { db ->
            db.execute("CREATE TABLE t_order (id INT NOT NULL)")

            db.useConnection { connection ->
                val error = assertFailsWith<IllegalArgumentException> {
                    resolve(connection, "t_order", requested = "nope")
                }
                assertTrue("nope" in error.message!! && "ID" in error.message!!)
            }
        }
    }

    @Test
    fun `指定了不可分区的类型时报错`() {
        H2Database.named("explicit_bad_type").use { db ->
            db.execute("CREATE TABLE t_order (id INT NOT NULL, name VARCHAR(50) NOT NULL)")

            db.useConnection { connection ->
                val error = assertFailsWith<IllegalArgumentException> {
                    resolve(connection, "t_order", requested = "name")
                }
                assertTrue("does not support partitioning" in error.message!!)
            }
        }
    }

    @Test
    fun `分区列上有 NULL 时仍允许分区，NULL 由读取端单独读`() {
        // Partition columns containing NULL are no longer rejected outright; NULL rows are read by the read side with an extra
        H2Database.named("nullable_partition").use { db ->
            db.execute("CREATE TABLE t_order (id INT)")
            db.execute("INSERT INTO t_order VALUES (1), (2), (NULL)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order", requested = "id")?.name)
            }
        }
    }

    @Test
    fun `可空列上没有 NULL 时允许分区`() {
        H2Database.named("nullable_but_clean").use { db ->
            db.execute("CREATE TABLE t_order (id INT)")
            db.execute("INSERT INTO t_order VALUES (1), (2)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order", requested = "id")?.name)
            }
        }
    }

    @Test
    fun `NULL 由读取端单独读，不再因分区列含 NULL 而拒绝`() {
        H2Database.named("nullable_with_where").use { db ->
            db.execute("CREATE TABLE t_order (id INT, grp INT)")
            db.execute("INSERT INTO t_order VALUES (1, 1), (NULL, 2)")

            db.useConnection { connection ->
                // Under both where conditions a partition column containing NULL is no longer rejected; the read side adds the
                assertEquals("ID", resolve(connection, "t_order", requested = "id", where = "grp = 1")?.name)
                assertEquals("ID", resolve(connection, "t_order", requested = "id", where = "grp >= 1")?.name)
            }
        }
    }

    @Test
    fun `非空列不需要额外查一次 NULL 数`() {
        H2Database.named("not_null_partition").use { db ->
            db.execute("CREATE TABLE t_order (id INT NOT NULL PRIMARY KEY)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order")?.name)
            }
        }
    }
}
