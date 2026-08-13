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
 * 类型映射的往返：读出来的 Row 再写回去，值必须一致。
 *
 * 顺带守住可序列化：读写器会跟着 DoFn 一起下发，捕获了不可序列化的对象就会在提交时炸掉，
 * 而这类问题单看代码很难发现。
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
    fun `所有支持的类型都能读出再写回`() {
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
            // 整型的具体宽度由驱动决定（H2 把 SMALLINT 的 class 报成 Integer），这里只校验取值
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
            // TIMESTAMP（不带时区）是墙上时间，转成 Instant 必然要按某个时区解释，
            // 这里跟驱动保持一致用 JVM 默认时区，所以断言也不能写死成 UTC
            assertEquals(
                java.sql.Timestamp.valueOf("2022-08-30 12:30:45").toInstant(),
                row.getLogicalTypeValue("C_TS", Instant::class.java),
            )

            // 写回另一张同构表，再读出来比对，验证 RowBinder 的换算与读取方向一致
            writeRow(db, row)
            assertEquals(row, readSingleRow(db, "t_copy"))
        }
    }

    @Test
    fun `NULL 值在往返中保持为 NULL`() {
        H2Database.named("roundtrip_null").use { db ->
            db.execute(ddl, ddl.replace("CREATE TABLE t ", "CREATE TABLE t_copy "))
            db.execute("INSERT INTO t (c_int) VALUES (NULL)")

            val row = readSingleRow(db)

            // getInt/getLong 遇到 NULL 会返回 0，必须靠 wasNull 才能分辨——这里就是在守这条
            assertNull(row.getInt32("C_INT"))
            assertNull(row.getString("C_VARCHAR"))
            assertNull(row.getBoolean("C_BOOL"))
            assertNull(row.getLogicalTypeValue("C_TS", Instant::class.java))

            writeRow(db, row)
            assertEquals(row, readSingleRow(db, "t_copy"))
        }
    }

    @Test
    fun `读写器可以被序列化下发`() {
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
    fun `数组列能读成 Beam 的 ARRAY`() {
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
                check(rs.next()) { "表 $table 没有数据" }
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
