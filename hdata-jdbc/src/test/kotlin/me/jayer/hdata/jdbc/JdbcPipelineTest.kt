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
 * ReadFromJdbc / WriteToJdbc 的端到端测试，跑在 DirectRunner + H2 内存库上。
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

    // ---------- 读 ----------

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

            // 不声明 partition_column，走主键自动探测；partition_num=4 切四段
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

            // 断言分区读真的生效了：H2 把表名存成大写，主键探测一旦按大小写匹配失败，
            // 就会悄悄退化成单分区读，而行数断言照样能过
            assertReadStrategy(graph, "PartitionedRead")
        }
    }

    @Test
    fun `分区列取到类型上界时不丢边界行`() {
        // 复现 INT 列最大值场景：最后一个查询块的上界是 toOffset(2147483647)+1 = 2147483648，
        // 回灌成 INT 会被 intValue 回绕成负数，于是 `col < 负数` 把 2147483647 那一行丢掉。
        // 修复后最后一个块只下推 `col >= ?`、不带 < 上界，边界行必须还在。
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
    fun `分区列上有 NULL 时拒绝执行，而不是悄悄漏掉那些行`() {
        H2Database.named("read_null_partition").use { db ->
            db.execute("CREATE TABLE t_plain (id INT, name VARCHAR(50))")
            db.execute("INSERT INTO t_plain VALUES (1, 'a'), (NULL, 'b')")

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
                            tables: ["t_plain"]
                            partition_column: id
                            partition_num: 2
                    """
                )
            }
            assertTrue("NULL" in error.message!!)
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
                // 让三张表的 id 不重叠，方便断言
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
            assertTrue("不支持分区" in error.message!!)
        }
    }

    // ---------- 写 ----------

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
            // 目标表少一列，插入必然失败
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

    // ---------- 死信 ----------

    @Test
    fun `开了死信后坏数据进死信流，好数据照常写入`() {
        H2Database.named("dead_letter").use { db ->
            db.createOrders(rows = 5)
            // 第 3 行的 name 置空，目标表 name 是 NOT NULL，只有这一行会失败
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

            // 4 条写进目标表，坏的那条被单独捞出来
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
                "错误类型应当是一个 SQLException",
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

    // ---------- 配置错误 ----------

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

    /** PCollection 的全名带着产出它的 transform 路径，用它判断走的是分区读还是单分区读。 */
    private fun assertReadStrategy(graph: me.jayer.hdata.core.graph.PipelineGraph, expected: String) {
        val name = graph.nodes.single { it.name == "Read" }.outputs.getValue("output").name
        assertTrue(expected in name, "期望读取方式包含[$expected]，实际的 PCollection 名字是: $name")
    }

    private fun build(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .second

    private fun run(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .also { it.first.run().waitUntilFinish() }
        .second
}
