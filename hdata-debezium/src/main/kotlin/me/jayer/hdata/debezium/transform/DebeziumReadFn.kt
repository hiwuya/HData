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
    // 队列与停止标记都是可空的：@Transient 字段在反序列化之后是 null（属性初始化器不会重跑），
    // 而 @Setup 失败时 @Teardown 照样会被调到。声明成非空（或 lateinit）的话，
    // 收尾时会抛 NPE / UninitializedPropertyAccessException，把真正的失败原因盖掉
    @Transient private var queue: LinkedBlockingQueue<Row?>? = null
    @Transient private var stopped: AtomicBoolean? = null

    @Setup
    fun setup() {
        queue = LinkedBlockingQueue()
        stopped = AtomicBoolean(false)
        val props = config.toProperties()
        val rows = checkNotNull(queue)
        val done = checkNotNull(stopped)
        val consumer = Consumer<SourceRecord> { record ->
            DebeziumRecords.toRow(record)?.let { rows.put(it) }
        }
        val completion = DebeziumEngine.CompletionCallback { _, _, _ ->
            done.set(true)
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
        val rows = checkNotNull(queue) { "Debezium 引擎未初始化" }
        val done = checkNotNull(stopped) { "Debezium 引擎未初始化" }
        val limit = config.maxRecords
        // 计的必须是**已经输出**的条数。之前看的是 emitted——那是引擎线程放进队列的条数，
        // 到达上限时队列里往往还压着一批，下面的收尾又会把它们全倒出去，
        // 结果 max_records 形同虚设，实际输出的条数比声明的多
        var count = 0L
        while (true) {
            val row = rows.poll(200, TimeUnit.MILLISECONDS)
            if (row != null) {
                out.output(row)
                count++
                if (limit != null && count >= limit) {
                    // 到量了就停：引擎还没吐完的那些是下一次作业的事
                    stopEngine()
                    return
                }
                continue
            }
            if (done.get()) break
        }
        // 引擎自己结束了（有界快照），把队列里剩下的收干净，同样不越过上限
        while (true) {
            val rest = rows.poll() ?: return
            out.output(rest)
            count++
            if (limit != null && count >= limit) {
                return
            }
        }
    }

    @Teardown
    fun teardown() {
        stopEngine()
    }

    @Synchronized
    private fun stopEngine() {
        // @Setup 还没跑到（或直接失败了）时 stopped 是 null，此时没有引擎要收
        if (stopped?.compareAndSet(false, true) == true) {
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
