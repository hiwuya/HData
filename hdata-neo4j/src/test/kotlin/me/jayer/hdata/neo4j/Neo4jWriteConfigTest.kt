package me.jayer.hdata.neo4j

import kotlin.test.Test
import kotlin.test.assertFailsWith

class Neo4jWriteConfigTest {

    @Test
    fun `statement 必填`() {
        assertFailsWith<IllegalArgumentException> { Neo4jWriteConfig(statement = "  ").validate() }
        Neo4jWriteConfig(statement = "CREATE (n)").validate()
    }
}
