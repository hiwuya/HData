package me.jayer.hdata.jdbc.internal

import me.jayer.hdata.jdbc.H2Database
import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Metadata probing. Every case here corresponds to a bug tracked down during the refactor.
 *
 * @author wuya
 * @date 2022-08-30
 */
class JdbcMetadataTest {

    @Test
    fun `primary key is ordered by KEY_SEQ, a composite key resolves to its first column`() {
        H2Database.named("pk_order").use { db ->
            // Deliberately make KEY_SEQ disagree with the declaration order: H2 emits ID(seq=2) before SUB(seq=1)
            db.execute("CREATE TABLE t (id INT, sub INT, PRIMARY KEY (sub, id))")

            db.useConnection { connection ->
                assertEquals(listOf("SUB", "ID"), JdbcMetadata.primaryKeyColumns(connection, "t"))
            }
        }
    }

    @Test
    fun `the primary key is still found when the table name's case doesn't match the dictionary`() {
        H2Database.named("pk_case").use { db ->
            // H2 stores unquoted identifiers in uppercase, so querying with the user's literal t_order finds nothing
            db.execute("CREATE TABLE t_order (id INT PRIMARY KEY)")

            db.useConnection { connection ->
                assertEquals(listOf("ID"), JdbcMetadata.primaryKeyColumns(connection, "t_order"))
                assertEquals(listOf("ID"), JdbcMetadata.primaryKeyColumns(connection, "T_ORDER"))
            }
        }
    }

    @Test
    fun `returns empty instead of throwing when there is no primary key`() {
        H2Database.named("pk_none").use { db ->
            db.execute("CREATE TABLE t (id INT)")

            db.useConnection { connection ->
                assertEquals(emptyList(), JdbcMetadata.primaryKeyColumns(connection, "t"))
                assertEquals(emptyList(), JdbcMetadata.primaryKeyColumns(connection, "no_such_table"))
            }
        }
    }

    @Test
    fun `a column with unknown nullability is treated as nullable`() {
        H2Database.named("nullable").use { db ->
            db.execute("CREATE TABLE t (a INT NOT NULL, b INT)")

            db.useConnection { connection ->
                val columns = JdbcMetadata.describeTable(connection, "t").associateBy { it.label }
                assertEquals(false, columns.getValue("A").nullable)
                assertEquals(true, columns.getValue("B").nullable)
            }
        }
    }

    @Test
    fun `duplicate column names give an actionable message instead of Beam's raw error`() {
        H2Database.named("dup_columns").use { db ->
            db.execute("CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)")

            db.useConnection { connection ->
                val columns = JdbcMetadata.describe(connection, "SELECT a.id, b.id FROM a, b")
                val error = assertFailsWith<IllegalArgumentException> { JdbcMetadata.toSchema(columns) }
                assertTrue("duplicate column names" in error.message!! && "alias" in error.message!!)
            }
        }
    }

    @Test
    fun `common types all map to a Beam schema`() {
        H2Database.named("types").use { db ->
            db.execute(
                """
                CREATE TABLE t (
                  c_bool BOOLEAN, c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT,
                  c_real REAL, c_double DOUBLE PRECISION, c_decimal DECIMAL(10,2),
                  c_varchar VARCHAR(50), c_clob CLOB, c_binary VARBINARY(16), c_blob BLOB,
                  c_date DATE, c_time TIME, c_ts TIMESTAMP
                )
                """.trimIndent()
            )

            db.useConnection { connection ->
                val (schema, readers) = JdbcMetadata.toSchema(JdbcMetadata.describeTable(connection, "t"))

                assertEquals(schema.fieldCount, readers.size)
                assertEquals(Schema.TypeName.BOOLEAN, schema.getField("C_BOOL").type.typeName)
                assertEquals(Schema.TypeName.INT32, schema.getField("C_INT").type.typeName)
                assertEquals(Schema.TypeName.INT64, schema.getField("C_BIG").type.typeName)
                assertEquals(Schema.TypeName.DOUBLE, schema.getField("C_DOUBLE").type.typeName)
                assertEquals(Schema.TypeName.DECIMAL, schema.getField("C_DECIMAL").type.typeName)
                assertEquals(Schema.TypeName.STRING, schema.getField("C_VARCHAR").type.typeName)
                assertEquals(Schema.TypeName.STRING, schema.getField("C_CLOB").type.typeName)
                assertEquals(Schema.TypeName.BYTES, schema.getField("C_BINARY").type.typeName)
                assertEquals(Schema.TypeName.BYTES, schema.getField("C_BLOB").type.typeName)
                assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("C_DATE").type.typeName)
                assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("C_TS").type.typeName)
            }
        }
    }

    @Test
    fun `an unsupported type gives an actionable message`() {
        H2Database.named("unsupported_type").use { db ->
            db.execute("CREATE TABLE t (c UUID)")

            db.useConnection { connection ->
                val columns = JdbcMetadata.describe(connection, "SELECT * FROM t")
                val error = assertFailsWith<IllegalArgumentException> { JdbcMetadata.toSchema(columns) }
                assertTrue("not supported yet" in error.message!! && "C" in error.message!!)
            }
        }
    }

    @Test
    fun `the partition range and NULL probe are fetched in one SQL statement that honors the where condition`() {
        H2Database.named("range").use { db ->
            db.execute("CREATE TABLE t (id INT, grp INT)")
            db.execute("INSERT INTO t VALUES (1, 1), (5, 1), (9, 2), (NULL, 1)")

            db.useConnection { connection ->
                val all = SelectSql("t")
                val probe = JdbcMetadata.partitionProbe(connection, all, "id")
                assertEquals(1, probe.min)
                assertEquals(9, probe.max)
                assertTrue(probe.hasNulls)

                // The where condition applies too: grp=2 has only the row id=9 and no NULL
                val filtered = SelectSql("t", conditions = listOf("grp = 2"))
                val filteredProbe = JdbcMetadata.partitionProbe(connection, filtered, "id")
                assertEquals(9, filteredProbe.min)
                assertEquals(9, filteredProbe.max)
                assertTrue(!filteredProbe.hasNulls)
            }
        }
    }

    @Test
    fun `an empty table's partition range is a pair of nulls with no NULL flag`() {
        H2Database.named("range_empty").use { db ->
            db.execute("CREATE TABLE t (id INT)")

            db.useConnection { connection ->
                val probe = JdbcMetadata.partitionProbe(connection, SelectSql("t"), "id")
                assertEquals(null, probe.min)
                assertEquals(null, probe.max)
                assertTrue(!probe.hasNulls)
            }
        }
    }
}
