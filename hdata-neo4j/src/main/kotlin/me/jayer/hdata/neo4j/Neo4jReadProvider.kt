package me.jayer.hdata.neo4j

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.neo4j.internal.parseSchemaFields
import me.jayer.hdata.neo4j.transform.Neo4jReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromNeo4j`：执行 Cypher 查询，把结果映射成行。
 *
 * 用 `Create.of(listOf(""))` 触发一次读取（有界快照），再经 DoFn 逐条执行查询并取数，
 * 连接在每个 DoFn 实例里独立建立，天然可序列化。
 *
 * @author wuya
 */
class Neo4jReadProvider : TypedTransformProvider<Neo4jReadConfig>(Neo4jReadConfig::class.java) {

    override fun identifier(): String = "ReadFromNeo4j"

    override fun description(): String = "从 Neo4j 读取（Cypher 查询）"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: Neo4jReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return Neo4jSource(config)
    }
}

private class Neo4jSource(private val config: Neo4jReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val schemaFields = parseSchemaFields(config.schemaFields)
        val schema = config.outputSchema()
        val trigger = begin.apply("Trigger", Create.of(listOf("")))
        return trigger.apply("Read", ParDo.of(Neo4jReadFn(config, schema, schemaFields)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
