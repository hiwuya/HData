package me.jayer.hdata.neo4j.transform

import me.jayer.hdata.neo4j.Neo4jReadConfig
import me.jayer.hdata.neo4j.internal.DriverFactory
import me.jayer.hdata.neo4j.internal.RealDriverFactory
import me.jayer.hdata.neo4j.internal.newSession
import me.jayer.hdata.neo4j.internal.recordToRow
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.neo4j.driver.Driver

/**
 * 从 Neo4j 读出全量查询结果（有界快照），每条记录按 `schemaFields` 映射成一行。
 *
 * 连接在每个 DoFn 实例里独立建立（`@Setup` 建、`@Teardown` 关），字段标记 `@Transient` 保证可序列化；
 * 测试可直接调用 [processElement]，此时会惰性建连。
 *
 * @author wuya
 */
class Neo4jReadFn(
    private val config: Neo4jReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
    private val driverFactory: DriverFactory = RealDriverFactory,
) : DoFn<String, Row>() {

    @Transient
    private var driver: Driver? = null

    @Setup
    fun setup() {
        driver = driverFactory.create(config)
    }

    @Teardown
    fun tearDown() {
        runCatching { driver?.close() }
        driver = null
    }

    @ProcessElement
    fun processElement(@Element element: String, receiver: OutputReceiver<Row>) {
        val d = driver ?: driverFactory.create(config).also { driver = it }
        newSession(d, config).use { session ->
            val result = session.run(config.query, config.parameters ?: emptyMap<String, Any>())
            while (result.hasNext()) {
                receiver.output(recordToRow(result.next(), schemaFields, schema))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
