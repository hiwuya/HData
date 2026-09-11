package me.jayer.hdata.jdbc

import me.jayer.hdata.core.HData
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * End-to-end tests for ReadFromJdbc / WriteToJdbc, running on DirectRunner with an H2 in-memory database.
 *
 * @author wuya
 * @date 2022-08-30
 */
class JdbcPipelineTest {

    private fun H2Database.createOrders(table: String = "t_order", rows: Int = 10) {
        execute(
            """
            CREATE TABLE $table (
              id INT PRIMARY KEY,
              name VARCHAR(50),
              amount DECIMAL(10, 2),
              paid BOOLEAN
            )
            """.trimIndent()
        )
        useConnection { connection ->
            connection.prepareStatement("INSERT INTO $table VALUES (?, ?, ?, ?)").use { ps ->
                (1..rows).forEach { i ->
                    ps.setInt(1, i)
                    ps.setString(2, "name-$i")
                    ps.setBigDecimal(3, java.math.BigDecimal("$i.50"))
                    ps.setBoolean(4, i % 2 == 0)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    private fun H2Database.createTarget(table: String = "t_target", notNullName: Boolean = false) {
        execute(
            """
            CREATE TABLE $table (
              id INT PRIMARY KEY,
              name VARCHAR(50)${if (notNullName) " NOT NULL" else ""},
              amount DECIMAL(10, 2),
              paid BOOLEAN
            )
            """.trimIndent()
        )
    }

    private fun readConfig(db: H2Database, extra: String = ""): String = """
            url: "${db.url}"
            user: "${H2Database.USER}"
            password: ""
$extra
    """.trimIndent()

    // ---------- read ----------

    @Test
    fun `按表读出全部行并推断出 schema`() {
        H2Database.named("read_all").use { db ->
            db.createOrders(rows = 5)

            val graph = run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 1 }
                          - { id: 2 }
                          - { id: 3 }
                          - { id: 4 }
                          - { id: 5 }
                """
            )

            val schema = graph.nodes.single { it.name == "Read" }.outputs.getValue("output").schema
            assertEquals(listOf("ID", "NAME", "AMOUNT", "PAID"), schema.fieldNames)
            assertEquals(Schema.FieldType.INT32, schema.getField("ID").type)
            assertEquals(Schema.FieldType.STRING.withNullable(true), schema.getField("NAME").type)
            assertEquals(Schema.FieldType.DECIMAL.withNullable(true), schema.getField("AMOUNT").type)
            assertEquals(Schema.FieldType.BOOLEAN.withNullable(true), schema.getField("PAID").type)
        }
    }

    @Test
    fun `where 条件下推到 SQL`() {
        H2Database.named("read_where").use { db ->
            db.createOrders(rows = 10)

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        where: "id > 8"
                        partition_num: 1
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 9 }
                          - { id: 10 }
                """
            )
        }
    }

    @Test
    fun `limit 下推成 SQL 的 LIMIT（最多读 N 行）`() {
        H2Database.named("read_limit").use { db ->
            db.createOrders(rows = 10)

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        limit: 3
                        partition_num: 1
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 1 }
                          - { id: 2 }
                          - { id: 3 }
                """
            )
        }
    }

    @Test
    fun `limit 与 where 同时下推`() {
        H2Database.named("read_limit_where").use { db ->
            db.createOrders(rows = 10)

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        where: "id > 8"
                        limit: 1
                        partition_num: 1
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 9 }
                """
            )
        }
    }

    @Test
    fun `query 模式带 limit 显式报错`() {
        H2Database.named("read_query_limit").use { db ->
            db.createOrders(rows = 10)
            val error = assertFailsWith<IllegalArgumentException> {
                build(
                    """
                    pipeline:
                      type: chain
                      transforms:
                        - type: ReadFromJdbc
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            query: "SELECT id FROM t_order"
                            limit: 5
                    """.trimIndent()
                )
            }
            assertTrue(error.message!!.contains("limit"), error.message)
        }
    }

    @Test
    fun `columns 只取指定列`() {
        H2Database.named("read_columns").use { db ->
            db.createOrders(rows = 2)

            val graph = build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        columns: ["id", "name"]
                        partition_num: 1
                """
            )

            val schema = graph.nodes.single { it.name == "Read" }.outputs.getValue("output").schema
            assertEquals(listOf("ID", "NAME"), schema.fieldNames)
        }
    }

    @Test
    fun `query 模式直接执行自定义 SQL`() {
        H2Database.named("read_query").use { db ->
            db.createOrders(rows = 10)
            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        query: "SELECT id AS order_id FROM t_order WHERE id <= 3"
                    - type: AssertEqual
                      config:
                        elements:
                          - { ORDER_ID: 1 }
                          - { ORDER_ID: 2 }
                          - { ORDER_ID: 3 }
                """
            )
        }
    }

    @Test
    fun `按主键自动分区并行读，结果不重不漏`() {
        H2Database.named("read_partitioned").use { db ->
            db.createOrders(rows = 20)

            // No partition_column declared, so primary key auto detection kicks in; partition_num=4 splits into four pieces
            val graph = run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 4
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements: ${(1..20).joinToString(", ", "[", "]") { "{ id: $it }" }}
                """
            )

            // Asserts that partitioned reads really took effect: H2 stores table names in uppercase, and once primary key detection
            // fails on the case mismatch it silently degrades to a single-partition read while the row count assertion still passes
            assertReadStrategy(graph, "PartitionedRead")
        }
    }

    @Test
    fun `分区列取到类型上界时不丢边界行`() {
        // Reproduces the INT column maximum scenario: the last query chunk's upper bound is toOffset(2147483647)+1 = 2147483648,
        // which intValue wraps into a negative number when converted back to INT, so `col < negative` drops the row 2147483647.
        // After the fix the last chunk only pushes down `col >= ?` with no `< ?` upper bound, so the boundary row must still be there.
        H2Database.named("read_boundary").use { db ->
            db.execute("CREATE TABLE t_boundary (id INT PRIMARY KEY)")
            db.execute("INSERT INTO t_boundary VALUES (1), (2147483647)")

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_boundary"]
                        partition_num: 2
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements: ${listOf(1, 2147483647).joinToString(", ", "[", "]") { "{ id: $it }" }}
                """
            )
        }
    }

    @Test
    fun `partition_num 为 1 时走单分区读`() {
        H2Database.named("read_single").use { db ->
            db.createOrders(rows = 3)

            val graph = build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                """
            )

            assertReadStrategy(graph, "Read/Read")
        }
    }

    @Test
    fun `没有主键又没指定分区列时退化为单分区读`() {
        H2Database.named("read_no_pk").use { db ->
            db.execute("CREATE TABLE t_plain (id INT, name VARCHAR(50))")
            db.execute("INSERT INTO t_plain VALUES (1, 'a'), (2, 'b')")

            val graph = run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_plain"]
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 1 }
                          - { id: 2 }
                """
            )

            assertReadStrategy(graph, "Read/Read")
        }
    }

    @Test
    fun `分区列上有 NULL 时读入 IS NULL 独立查询，不丢那些行`() {
        // Matching Trino: NULL rows go into a dedicated split (here a `col IS NULL` query) instead of failing outright or being
        // silently skipped as before.
        H2Database.named("read_null_partition").use { db ->
            db.execute("CREATE TABLE t_plain (id INT, name VARCHAR(50))")
            db.execute("INSERT INTO t_plain VALUES (1, 'a'), (2, 'b'), (20, 'c'), (NULL, 'null-row')")

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      name: Read
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_plain"]
                        partition_column: id
                        partition_num: 2
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                          name: NAME
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 1, name: "a" }
                          - { id: 2, name: "b" }
                          - { id: 20, name: "c" }
                          - { id: null, name: "null-row" }
                """
            )
        }
    }

    @Test
    fun `分区列全为 NULL 时只执行 NULL 查询块`() {
        H2Database.named("read_all_null_partition").use { db ->
            db.execute("CREATE TABLE t_plain (id INT, name VARCHAR(50))")
            db.execute("INSERT INTO t_plain VALUES (NULL, 'a'), (NULL, 'b')")

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_plain"]
                        partition_column: id
                        partition_num: 2
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                          name: NAME
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: null, name: "a" }
                          - { id: null, name: "b" }
                """
            )
        }
    }

    @Test
    fun `空表分区读不产生任何行也不报错`() {
        H2Database.named("read_empty").use { db ->
            db.createOrders(rows = 0)
            db.createTarget()

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 4
                    - type: WriteToJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_target
                """
            )

            assertEquals(0, db.count("t_target"))
        }
    }

    @Test
    fun `显式分区列同样不重不漏`() {
        H2Database.named("read_partition_column").use { db ->
            db.createOrders(rows = 15)

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_column: id
                        partition_num: 3
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements: ${(1..15).joinToString(", ", "[", "]") { "{ id: $it }" }}
                """
            )
        }
    }

    @Test
    fun `分区列与 where 同时生效`() {
        H2Database.named("read_partition_where").use { db ->
            db.createOrders(rows = 20)

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        where: "id > 15"
                        partition_column: id
                        partition_num: 2
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements: ${(16..20).joinToString(", ", "[", "]") { "{ id: $it }" }}
                """
            )
        }
    }

    @Test
    fun `多表按区间语法展开后合并读出`() {
        H2Database.named("read_table_range").use { db ->
            (0..2).forEach { index ->
                db.createOrders(table = "t_order_0$index", rows = 2)
                // Keep the ids of the three tables disjoint to make assertion easy
                db.execute("UPDATE t_order_0$index SET id = id + ${index * 10}")
            }

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order_${'$'}{00-02}"]
                        partition_num: 1
                    - type: MapToFields
                      config:
                        fields:
                          id: ID
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 1 }
                          - { id: 2 }
                          - { id: 11 }
                          - { id: 12 }
                          - { id: 21 }
                          - { id: 22 }
                """
            )
        }
    }

    @Test
    fun `分区列不存在时报错并列出可用列`() {
        H2Database.named("read_bad_partition").use { db ->
            db.createOrders()

            val error = assertFailsWith<IllegalArgumentException> {
                build(
                    """
                    pipeline:
                      type: chain
                      transforms:
                        - type: ReadFromJdbc
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            tables: ["t_order"]
                            partition_column: nope
                    """
                )
            }
            assertTrue("nope" in error.message!! && "ID" in error.message!!)
        }
    }

    @Test
    fun `分区列类型不支持时报错并列出支持的类型`() {
        H2Database.named("read_bad_partition_type").use { db ->
            db.createOrders()

            val error = assertFailsWith<IllegalArgumentException> {
                build(
                    """
                    pipeline:
                      type: chain
                      transforms:
                        - type: ReadFromJdbc
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            tables: ["t_order"]
                            partition_column: name
                    """
                )
            }
            assertTrue("does not support partitioning" in error.message!!)
        }
    }

    // ---------- write ----------

    @Test
    fun `写入端把行落到目标表`() {
        H2Database.named("write_basic").use { db ->
            db.createOrders(rows = 5)
            db.createTarget()

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                    - type: WriteToJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_target
                """
            )

            assertEquals(5, db.count("t_target"))
            assertEquals(listOf(1, 2, 3, 4, 5), db.queryColumn<Int>("SELECT id FROM t_target ORDER BY id"))
            assertEquals(
                listOf("name-1", "name-2", "name-3", "name-4", "name-5"),
                db.queryColumn<String>("SELECT name FROM t_target ORDER BY id"),
            )
        }
    }

    @Test
    fun `batch_size 小于总行数时分多批写入`() {
        H2Database.named("write_batched").use { db ->
            db.createOrders(rows = 25)
            db.createTarget()

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                    - type: WriteToJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_target
                        batch_size: 4
                """
            )

            assertEquals(25, db.count("t_target"))
        }
    }

    @Test
    fun `没开死信时写入失败让作业失败`() {
        H2Database.named("write_fail_fast").use { db ->
            db.createOrders(rows = 3)
            // The target table is missing one column, so the insert must fail
            db.execute("CREATE TABLE t_target (id INT PRIMARY KEY)")

            assertFailsWith<Throwable> {
                run(
                    """
                    pipeline:
                      type: chain
                      transforms:
                        - type: ReadFromJdbc
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            tables: ["t_order"]
                            partition_num: 1
                        - type: WriteToJdbc
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            table: t_target
                    """
                )
            }

            assertEquals(0, db.count("t_target"))
        }
    }

    // ---------- dead letter ----------

    @Test
    fun `开了死信后坏数据进死信流，好数据照常写入`() {
        H2Database.named("dead_letter").use { db ->
            db.createOrders(rows = 5)
            // Blank out the name of row 3; name is NOT NULL in the target table, so only this row fails
            db.execute("UPDATE t_order SET name = NULL WHERE id = 3")
            db.createTarget(notNullName = true)
            db.execute("CREATE TABLE t_rejected (id INT, name VARCHAR(50), amount DECIMAL(10,2), paid BOOLEAN)")

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                    - type: WriteToJdbc
                      name: WriteTarget
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_target
                        error_handling:
                          output: rejected
                  extra_transforms:
                    - type: StripErrorMetadata
                      name: Recovered
                      input: WriteTarget.rejected
                    - type: WriteToJdbc
                      name: WriteRejected
                      input: Recovered
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_rejected
                """
            )

            // 4 rows land in the target table, and the bad one is pulled out separately
            assertEquals(4, db.count("t_target"))
            assertEquals(listOf(1, 2, 4, 5), db.queryColumn<Int>("SELECT id FROM t_target ORDER BY id"))

            assertEquals(1, db.count("t_rejected"))
            assertEquals(listOf(3), db.queryColumn<Int>("SELECT id FROM t_rejected"))
        }
    }

    @Test
    fun `死信记录带上错误类型与 transform 名字`() {
        H2Database.named("dead_letter_meta").use { db ->
            db.createOrders(rows = 2)
            db.execute("UPDATE t_order SET name = NULL WHERE id = 1")
            db.createTarget(notNullName = true)
            db.execute("CREATE TABLE t_errors (error_type VARCHAR(200), error_message VARCHAR(1000), transform VARCHAR(100))")

            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                    - type: WriteToJdbc
                      name: WriteTarget
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_target
                        error_handling:
                          output: rejected
                  extra_transforms:
                    - type: MapToFields
                      name: ErrorMeta
                      input: WriteTarget.rejected
                      config:
                        fields:
                          error_type: error_type
                          error_message: error_message
                          transform: transform
                    - type: WriteToJdbc
                      name: WriteErrors
                      input: ErrorMeta
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_errors
                """
            )

            assertEquals(1, db.count("t_errors"))
            assertEquals(
                listOf("WriteTarget"),
                db.queryColumn<String>("SELECT transform FROM t_errors"),
            )
            assertTrue(
                db.queryColumn<String>("SELECT error_type FROM t_errors").single().contains("SQL"),
                "the error type should be a SQLException",
            )
        }
    }

    @Test
    fun `死信 schema 保留原始记录的字段`() {
        H2Database.named("dead_letter_schema").use { db ->
            db.createOrders(rows = 1)
            db.createTarget(notNullName = true)

            val graph = build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        partition_num: 1
                    - type: WriteToJdbc
                      name: WriteTarget
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        table: t_target
                        error_handling:
                          output: rejected
                  extra_transforms:
                    - type: StripErrorMetadata
                      name: Recovered
                      input: WriteTarget.rejected
                """
            )

            val errors = graph.nodes.single { it.name == "WriteTarget" }.outputs.getValue("errors").schema
            assertEquals(listOf("element", "error_type", "error_message", "transform"), errors.fieldNames)

            val recovered = graph.nodes.single { it.name == "Recovered" }.outputs.getValue("output").schema
            assertEquals(listOf("ID", "NAME", "AMOUNT", "PAID"), recovered.fieldNames)
        }
    }

    @Test
    fun `没有下游消费死信流时构图直接失败`() {
        H2Database.named("dead_letter_unconsumed").use { db ->
            db.createOrders(rows = 1)
            db.createTarget()

            val error = assertFailsWith<HDataException> {
                build(
                    """
                    pipeline:
                      type: chain
                      transforms:
                        - type: ReadFromJdbc
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            tables: ["t_order"]
                            partition_num: 1
                        - type: WriteToJdbc
                          name: WriteTarget
                          config:
                            url: "${db.url}"
                            user: "sa"
                            password: ""
                            table: t_target
                            error_handling:
                              output: rejected
                    """
                )
            }
            assertTrue("WriteTarget.rejected" in error.message!!)
        }
    }

    // ---------- config errors ----------

    @Test
    fun `连不上库时报错`() {
        assertFailsWith<Throwable> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "jdbc:h2:mem:definitely_not_there;IFEXISTS=TRUE"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                """
            )
        }
    }

    @Test
    fun `config 里拼错的键会被拒绝`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "jdbc:h2:mem:x"
                        tables: ["t"]
                        partiton_num: 4
                """
            )
        }
        assertTrue("partiton_num" in error.message!!)
    }

    /** The full name of a PCollection carries the transform path that produced it; use it to tell partitioned from single-partition reads. */
    private fun assertReadStrategy(graph: me.jayer.hdata.core.graph.PipelineGraph, expected: String) {
        val name = graph.nodes.single { it.name == "Read" }.outputs.getValue("output").name
        assertTrue(expected in name, "expected the read strategy to contain [$expected], but the actual PCollection name is: $name")
    }

    private fun build(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .second

    @Test
    fun `聚合下推翻译成 DB 原生聚合 SQL 返回单行`() {
        // count/min/max/sum are computed on the source side, so Beam only receives the single aggregated row
        H2Database.named("read_agg").use { db ->
            db.createOrders(rows = 5) // id 1..5
            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        aggregations: ["count", "min:id", "max:id", "sum:id"]
                    - type: AssertEqual
                      config:
                        elements:
                          - { count: 5, min_id: 1, max_id: 5, sum_id: 15 }
                """
            )
        }
    }

    @Test
    fun `聚合下推带 where 先过滤再聚合`() {
        // where and aggregation take effect together: filter on id > 3 first, then aggregate the remaining 3 rows
        H2Database.named("read_agg_where").use { db ->
            db.createOrders(rows = 5)
            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${db.url}"
                        user: "sa"
                        password: ""
                        tables: ["t_order"]
                        where: "id > 3"
                        aggregations: ["count", "min:id", "max:id"]
                    - type: AssertEqual
                      config:
                        elements:
                          - { count: 2, min_id: 4, max_id: 5 }
                """
            )
        }
    }

    private fun run(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .also { it.first.run().waitUntilFinish() }
        .second
}
