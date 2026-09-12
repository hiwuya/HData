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
    fun `reads every row from a table and infers the schema`() {
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
    fun `where condition is pushed down into SQL`() {
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
    fun `limit is pushed down as SQL's LIMIT (reads at most N rows)`() {
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
    fun `limit and where are pushed down together`() {
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
    fun `query mode with limit errors explicitly`() {
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
    fun `columns reads only the specified columns`() {
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
    fun `query mode runs a custom SQL statement directly`() {
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
    fun `auto-partitions by primary key for parallel reads, with no overlap or gap`() {
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
    fun `no boundary row is lost when the partition column hits the type's upper bound`() {
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
    fun `partition_num of 1 takes the single-partition read path`() {
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
    fun `degrades to a single-partition read when there is no primary key and no partition column specified`() {
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
    fun `a NULL in the partition column reads through a separate IS NULL query, so those rows aren't lost`() {
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
    fun `only the NULL query chunk runs when the partition column is entirely NULL`() {
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
    fun `a partitioned read of an empty table produces no rows and no error`() {
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
                        allow_duplicate_replay: true
                """
            )

            assertEquals(0, db.count("t_target"))
        }
    }

    @Test
    fun `an explicit partition column is likewise free of overlap or gap`() {
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
    fun `the partition column and where take effect together`() {
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
    fun `multiple tables expanded by range syntax are read and merged together`() {
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
    fun `errors and lists the available columns when the partition column doesn't exist`() {
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
    fun `errors and lists supported types when the partition column's type isn't supported`() {
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
    fun `the write side lands rows into the target table`() {
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
                        allow_duplicate_replay: true
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
    fun `writes in several batches when batch_size is smaller than the total row count`() {
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
                        allow_duplicate_replay: true
                        batch_size: 4
                """
            )

            assertEquals(25, db.count("t_target"))
        }
    }

    @Test
    fun `a write failure fails the job when the dead letter isn't enabled`() {
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
    fun `bad rows go to the dead-letter stream and good rows still get written once the dead letter is enabled`() {
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
                        allow_duplicate_replay: true
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
                        allow_duplicate_replay: true
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
    fun `the dead-letter record carries the error type and transform name`() {
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
                        allow_duplicate_replay: true
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
                        allow_duplicate_replay: true
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
    fun `the dead-letter schema keeps the original record's fields`() {
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
                        allow_duplicate_replay: true
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
    fun `graph construction fails outright when nothing downstream consumes the dead-letter stream`() {
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
    fun `errors when the database can't be reached`() {
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
    fun `a misspelled key in the config is rejected`() {
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
    fun `aggregation pushdown translates into native DB aggregate SQL and returns a single row`() {
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
    fun `aggregation pushdown filters with where before aggregating`() {
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
