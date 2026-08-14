package me.jayer.hdata.debezium

import me.jayer.hdata.debezium.internal.DebeziumRecords
import me.jayer.hdata.debezium.transform.DebeziumReadFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

/**
 * 端到端：用 Debezium 自带的、不需要数据库的 `SimpleSourceConnector` 真正启动嵌入式引擎，
 * 验证「引擎启动 → Consumer 收事件 → 队列 → DoFn 输出」整条链路。`max_records=1` 让 DoFn
 * 收到第一条就停掉引擎，避免无库场景下连接器不主动退出的长时间空转。
 * 无法在此环境验证真实 MySQL/Postgres 的 binlog/WAL 捕获，那部分交由用户在生产库上验证。
 */
class DebeziumPipelineTest {

    @Test
    fun `SimpleSourceConnector 端到端跑通`() {
        val config = DebeziumReadConfig(
            connector = "simple",
            connectorClass = "io.debezium.connector.simple.SimpleSourceConnector",
            name = "test",
            maxRecords = 3,
            extra = mapOf(
                "topic.name" to "simple",
                "record.count.per.batch" to "3",
                "batch.count" to "1",
                "include.timestamp" to "false",
                "offset.flush.interval.ms" to "1000",
            ),
        )
        val p = Pipeline.create()
        val trigger = p.apply(Create.of(listOf("")))
        val out = trigger.apply(ParDo.of(DebeziumReadFn(config))).setRowSchema(DebeziumRecords.SCHEMA)
        PAssert.that(out).satisfies { rows ->
            assertTrue(rows.toList().size >= 1)
            null
        }
        p.run()
    }
}
