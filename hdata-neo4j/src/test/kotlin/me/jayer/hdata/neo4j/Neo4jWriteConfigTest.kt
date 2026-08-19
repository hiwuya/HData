package me.jayer.hdata.neo4j

import kotlin.test.Test
import kotlin.test.assertFailsWith

class Neo4jWriteConfigTest {

    @Test
    fun `statement 必填`() {
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "  ").validate() }
        Neo4jWriteConfig(statement = "CREATE (n)").validate()
    }

    @Test
    fun `batch_size 必须为正`() {
        // batch_size 控制一个事务提交多少行，<=0 会让 flush 永远凑不满、数据写不进去
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "CREATE (n)", batchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "CREATE (n)", batchSize = -1).validate() }
        Neo4jWriteConfig(statement = "CREATE (n)", batchSize = 1).validate()
    }

    @Test
    fun `连接配置会在构图阶段校验`() {
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(uri = "", statement = "CREATE (n)").validate() }
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(user = "", statement = "CREATE (n)").validate() }
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(database = " ", statement = "CREATE (n)").validate() }
    }

    @Test
    fun `显式参数映射必须覆盖且只覆盖 Cypher 占位符`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jWriteConfig(
                statement = "CREATE (n {name: \$name, age: \$age})",
                parameters = mapOf("name" to "person_name"),
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            Neo4jWriteConfig(
                statement = "CREATE (n {name: \$name})",
                parameters = mapOf("name" to "person_name", "unused" to "x"),
            ).validate()
        }
        Neo4jWriteConfig(
            statement = "CREATE (n {name: \$name})",
            parameters = mapOf("name" to "person_name"),
        ).validate()
    }
}
