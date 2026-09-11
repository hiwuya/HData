package me.jayer.hdata.jdbc.internal

import me.jayer.hdata.jdbc.H2Database
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Round trip of the type mapping: a Row read out and written back must keep the same values.
 *
 * It also guards serializability: readers and writers ship with the DoFn, so capturing a non-serializable object blows up at
 * submission time, and that kind of problem is hard to spot by reading the code alone.
 *
 * @author wuya
 * @date 2022-08-30
 */
class TypeMappingsTest {

    private val ddl = """
        CREATE TABLE t (
          c_bool BOOLEAN, c_small SMALLINT, c_int INT, c_big BIGINT,
          c_real REAL, c_double DOUBLE PRECISION, c_decimal DECIMAL(12,3),
          c_varchar VARCHAR(50), c_clob CLOB, c_binary VARBINARY(16),
          c_date DATE, c_time TIME, c_ts TIMESTAMP
        )
    """.trimIndent()

    @Test
    fun `every supported type can be read out and written back`() {
        H2Database.named("roundtrip").use { db ->
            db.execute(ddl, ddl.replace("CREATE TABLE t ", "CREATE TABLE t_copy "))
            db.useConnection { connection ->
                connection.prepareStatement("INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)").use { ps ->
                    ps.setBoolean(1, true)
                    ps.setShort(2, 7)
                    ps.setInt(3, 12345)
                    ps.setLong(4, 9_000_000_000L)
                    ps.setFloat(5, 1.5f)
                    ps.setDouble(6, 2.25)
                    ps.setBigDecimal(7, BigDecimal("123.456"))
                    ps.setString(8, "hello")
                    ps.setString(9, "a long text")
                    ps.setBytes(10, byteArrayOf(1, 2, 3))
                    ps.setDate(11, java.sql.Date.valueOf("2022-08-30"))
                    ps.setTime(12, java.sql.Time.valueOf("12:30:45"))
                    ps.setTimestamp(13, java.sql.Timestamp.valueOf("2022-08-30 12:30:45"))
                    ps.executeUpdate()
                }
            }

            val row = readSingleRow(db)

            assertEquals(true, row.getBoolean("C_BOOL"))
            // The concrete integer width is up to the driver (H2 reports the class of SMALLINT as Integer), so only check the value
            assertEquals(7L, row.getValue<Number>("C_SMALL").toLong())
            assertEquals(12345, row.getInt32("C_INT"))
            assertEquals(9_000_000_000L, row.getInt64("C_BIG"))
            assertEquals(1.5f, row.getFloat("C_REAL"))
            assertEquals(2.25, row.getDouble("C_DOUBLE"))
            assertEquals(BigDecimal("123.456"), row.getDecimal("C_DECIMAL"))
            assertEquals("hello", row.getString("C_VARCHAR"))
            assertEquals("a long text", row.getString("C_CLOB"))
            assertContentEquals(byteArrayOf(1, 2, 3), row.getBytes("C_BINARY"))
            assertEquals(LocalDate.of(2022, 8, 30), row.getLogicalTypeValue("C_DATE", LocalDate::class.java))
            assertEquals(LocalTime.of(12, 30, 45), row.getLogicalTypeValue("C_TIME", LocalTime::class.java))
            // TIMESTAMP (without time zone) is wall-clock time, so converting it to an Instant must pick some time zone;
            // we stay consistent with the driver and use the JVM default time zone, so the assertion must not hardcode UTC either
            assertEquals(
                java.sql.Timestamp.valueOf("2022-08-30 12:30:45").toInstant(),
                row.getLogicalTypeValue("C_TS", Instant::class.java),
            )

            // Write into another table with the same structure and read it back to compare, verifying that RowBinder converts consistently with the read direction
            writeRow(db, row)
            assertEquals(row, readSingleRow(db, "t_copy"))
        }
    }

    @Test
    fun `a NULL value stays NULL through the round trip`() {
        H2Database.named("roundtrip_null").use { db ->
            db.execute(ddl, ddl.replace("CREATE TABLE t ", "CREATE TABLE t_copy "))
            db.execute("INSERT INTO t (c_int) VALUES (NULL)")

            val row = readSingleRow(db)

            // getInt/getLong return 0 for NULL, so only wasNull can tell them apart — this is what that line guards
            assertNull(row.getInt32("C_INT"))
            assertNull(row.getString("C_VARCHAR"))
            assertNull(row.getBoolean("C_BOOL"))
            assertNull(row.getLogicalTypeValue("C_TS", Instant::class.java))

            writeRow(db, row)
            assertEquals(row, readSingleRow(db, "t_copy"))
        }
    }

    @Test
    fun `readers and writers can be serialized and shipped`() {
        H2Database.named("serializable").use { db ->
            db.execute(ddl)

            db.useConnection { connection ->
                val (schema, readers) = JdbcMetadata.toSchema(JdbcMetadata.describeTable(connection, "t"))

                SerializableUtils.ensureSerializable(RowMapper(schema, readers))
                SerializableUtils.ensureSerializable(RowBinder.of(schema))
            }
        }
    }

    @Test
    fun `an array column reads into Beam's ARRAY`() {
        H2Database.named("array").use { db ->
            db.execute("CREATE TABLE t (ids INTEGER ARRAY)")
            db.useConnection { connection ->
                connection.prepareStatement("INSERT INTO t VALUES (?)").use { ps ->
                    ps.setArray(1, connection.createArrayOf("INTEGER", arrayOf(1, 2, 3)))
                    ps.executeUpdate()
                }
            }

            db.useConnection { connection ->
                val columns = JdbcMetadata.describeTable(connection, "t")
                val (schema, readers) = JdbcMetadata.toSchema(columns)
                assertEquals(Schema.TypeName.ARRAY, schema.getField("IDS").type.typeName)

                val row = connection.prepareStatement("SELECT * FROM t").use { ps ->
                    ps.executeQuery().use { rs ->
                        rs.next()
                        RowMapper(schema, readers).map(rs)
                    }
                }
                assertEquals(listOf(1, 2, 3), row.getArray<Int>("IDS"))
            }
        }
    }

    private fun readSingleRow(db: H2Database, table: String = "t"): Row = db.useConnection { connection ->
        val (schema, readers) = JdbcMetadata.toSchema(JdbcMetadata.describeTable(connection, table))
        connection.prepareStatement("SELECT * FROM $table").use { ps ->
            ps.executeQuery().use { rs ->
                check(rs.next()) { "table $table has no data" }
                RowMapper(schema, readers).map(rs)
            }
        }
    }

    private fun writeRow(db: H2Database, row: Row) = db.useConnection { connection ->
        val sql = InsertSql.render("t_copy", row.schema.fieldNames)
        connection.prepareStatement(sql).use { ps ->
            RowBinder.of(row.schema).bind(ps, row)
            ps.executeUpdate()
        }
    }
}
