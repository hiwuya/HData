package me.jayer.hdata.neo4j

import kotlin.test.Test
import kotlin.test.assertFailsWith

class Neo4jWriteConfigTest {

    @Test
    fun `statement is required`() {
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "  ").validate() }
        Neo4jWriteConfig(statement = "CREATE (n)").validate()
    }

    @Test
    fun `batch_size must be positive`() {
        // batch_size controls how many rows one transaction commits; <= 0 would mean flush never fills up and nothing ever gets written
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "CREATE (n)", batchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "CREATE (n)", batchSize = -1).validate() }
        Neo4jWriteConfig(statement = "CREATE (n)", batchSize = 1).validate()
    }

    @Test
    fun `connection config is validated at graph-construction time`() {
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(uri = "", statement = "CREATE (n)").validate() }
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(user = "", statement = "CREATE (n)").validate() }
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(database = " ", statement = "CREATE (n)").validate() }
    }

    @Test
    fun `an explicit parameter mapping must cover exactly the Cypher placeholders`() {
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
