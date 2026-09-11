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
    fun `auto-selects the primary key's first column when unspecified`() {
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
    fun `degrades to no partitioning with a warning when there is no primary key`() {
        H2Database.named("no_pk").use { db ->
            db.execute("CREATE TABLE t_order (id INT)")

            db.useConnection { connection ->
                assertNull(resolve(connection, "t_order"))
            }
        }
    }

    @Test
    fun `degrades to no partitioning when the primary key's type doesn't support partitioning`() {
        H2Database.named("pk_unsupported").use { db ->
            db.execute("CREATE TABLE t_order (id VARCHAR(50) PRIMARY KEY)")

            db.useConnection { connection ->
                assertNull(resolve(connection, "t_order"))
            }
        }
    }

    @Test
    fun `an explicit choice is case-insensitive and returns the column name as stored`() {
        H2Database.named("explicit_case").use { db ->
            db.execute("CREATE TABLE t_order (id INT NOT NULL)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order", requested = "id")?.name)
                assertEquals("ID", resolve(connection, "t_order", requested = "ID")?.name)
            }
        }
    }

    @Test
    fun `errors and lists the available columns when a nonexistent column is specified`() {
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
    fun `errors when a type that can't be partitioned is specified`() {
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
    fun `partitioning is still allowed when the partition column has a NULL, the read side reads it separately`() {
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
    fun `partitioning is allowed on a nullable column with no actual NULLs`() {
        H2Database.named("nullable_but_clean").use { db ->
            db.execute("CREATE TABLE t_order (id INT)")
            db.execute("INSERT INTO t_order VALUES (1), (2)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order", requested = "id")?.name)
            }
        }
    }

    @Test
    fun `NULL is read separately by the read side, a NULL in the partition column no longer causes rejection`() {
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
    fun `a NOT NULL column skips the extra NULL-count query`() {
        H2Database.named("not_null_partition").use { db ->
            db.execute("CREATE TABLE t_order (id INT NOT NULL PRIMARY KEY)")

            db.useConnection { connection ->
                assertEquals("ID", resolve(connection, "t_order")?.name)
            }
        }
    }
}
