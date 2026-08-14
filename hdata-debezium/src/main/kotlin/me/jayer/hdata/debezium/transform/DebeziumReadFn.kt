package me.jayer.hdata.debezium.transform

import io.debezium.embedded.EmbeddedEngine
import io.debezium.engine.DebeziumEngine
import me.jayer.hdata.debezium.DebeziumReadConfig
import me.jayer.hdata.debezium.internal.DebeziumRecords
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.kafka.connect.source.SourceRecord
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

/**
 * 用 Debezium 嵌入式引擎做变更捕获的源 DoFn。
 *
 * - `@Setup` 里构建并启动 [EmbeddedEngine]（阻塞跑在独立线程上），变更事件经 Consumer
 *   转成 [Row] 后塞进一个阻塞队列；
 * - `@ProcessElement` 在触发元素上循环从队列取行并 `output`，直到引擎结束（有界快照）或
 *   达到 `max_records`；
 * - `@Teardown` 中断引擎线程并回收，不阻塞等待连接器优雅退出。
 *
 * 引擎、线程、队列都标记 `@Transient`，不参与序列化；连接相关的对象只在 worker 上
 * 通过 `@Setup` 构建。
 */
class DebeziumReadFn(
    private val config: DebeziumReadConfig,
) : DoFn<String, Row>() {

    @Transient private var engine: EmbeddedEngine? = null
    @Transient private var thread: Thread? = null
    @Transient private lateinit var queue: LinkedBlockingQueue<Row?>
    @Transient private lateinit var stopped: AtomicBoolean
    @Transient private lateinit var emitted: AtomicInteger

    @Setup
    fun setup() {
        queue = LinkedBlockingQueue()
        stopped = AtomicBoolean(false)
        emitted = AtomicInteger(0)
        val props = config.toProperties()
        val consumer = Consumer<SourceRecord> { record ->
            val row = DebeziumRecords.toRow(record)
            if (row != null) {
                emitted.incrementAndGet()
                queue.put(row)
            }
        }
        val completion = DebeziumEngine.CompletionCallback { _, _, _ ->
            stopped.set(true)
        }
        engine = EmbeddedEngine.EngineBuilder()
            .using(props)
            .notifying(consumer)
            .using(completion)
            .build() as EmbeddedEngine
        thread = Thread { engine!!.run() }.also { it.name = "debezium-engine"; it.start() }
    }

    @ProcessElement
    fun processElement(@Element element: String, out: OutputReceiver<Row>) {
        while (true) {
            val row = queue.poll(200, TimeUnit.MILLISECONDS)
            if (row != null) {
                out.output(row)
                if (config.maxRecords != null && emitted.get() >= config.maxRecords!!) {
                    stopped.set(true)
                    break
                }
                continue
            }
            if (stopped.get()) break
        }
        if (stopped.get()) {
            var rest = queue.poll()
            while (rest != null) {
                out.output(rest)
                rest = queue.poll()
            }
        }
    }

    @Teardown
    fun teardown() {
        stopEngine()
    }

    @Synchronized
    private fun stopEngine() {
        if (stopped.compareAndSet(false, true)) {
            try {
                thread?.interrupt()
            } catch (_: Throwable) {
            }
            try {
                engine?.close()
            } catch (_: Throwable) {
            }
            try {
                thread?.join(5000)
            } catch (_: Throwable) {
            }
        }
    }
}
