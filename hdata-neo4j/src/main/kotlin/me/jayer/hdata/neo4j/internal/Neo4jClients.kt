package me.jayer.hdata.neo4j.internal

import me.jayer.hdata.neo4j.Neo4jConnectionConfig
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import java.io.Serializable

/**
 * Serializable Driver factory for injecting test implementations; see the Neo4j read and write functions.
 *
 * @author wuya
 */
fun interface DriverFactory : Serializable {
    fun create(config: Neo4jConnectionConfig): Driver
}

object RealDriverFactory : DriverFactory {
    override fun create(config: Neo4jConnectionConfig): Driver =
        GraphDatabase.driver(config.uri, AuthTokens.basic(config.user, config.password))
}

fun newSession(driver: Driver, config: Neo4jConnectionConfig): Session =
    if (config.database != null) driver.session(SessionConfig.forDatabase(config.database))
    else driver.session()
