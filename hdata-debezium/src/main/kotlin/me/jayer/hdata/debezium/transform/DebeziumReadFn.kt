package me.jayer.hdata.debezium.transform

import io.debezium.embedded.EmbeddedEngine
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.StopEngineException
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.debezium.DebeziumReadConfig
import me.jayer.hdata.debezium.internal.DebeziumRecords
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.kafka.connect.source.SourceRecord
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * 用 Debezium 嵌入式引擎做变更捕获的源 DoFn。
 *
 * - `@Setup` 里构建并启动 [EmbeddedEngine]（阻塞跑在独立线程上），变更事件经 Consumer
 *   转成 [Row] 后塞进一个阻塞队列；
 * - `@ProcessElement` 在触发元素上循环从队列取行并 `output`，直到引擎结束（有界快照）或
 *   达到 `max_records`；
 * - `max_records` 在引擎消费回调里精确计数，第 N 条有效记录入队后用 Debezium 自带的
 *   [StopEngineException] 正常结束引擎，让 offset 的最终提交与连接器关闭保持原生时序；
 * - `@Teardown` 只为作业取消、异常等路径兜底关闭引擎。
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
    @Transient private var failure: AtomicReference<Throwable?>? = null

    @Setup
    fun setup() {
        queue = LinkedBlockingQueue()
        stopped = AtomicBoolean(false)
        failure = AtomicReference(null)
        val props = config.toProperties()
        val rows = checkNotNull(queue)
        val done = checkNotNull(stopped)
        val engineFailure = checkNotNull(failure)
        val enqueued = AtomicLong()
        val consumer = Consumer<SourceRecord> { record ->
            DebeziumRecords.toRow(record)?.let { row ->
                rows.put(row)
                if (config.maxRecords?.let { enqueued.incrementAndGet() >= it } == true) {
                    // 让 EmbeddedEngine 从自己的 handler 路径正常退出；外部先 interrupt 再 close 会打断
                    // 正在进行的 offset flush，随后 finally 再 flush 时触发 beginFlush 重入错误。
                    throw StopEngineException("已达到 max_records=${config.maxRecords}")
                }
            }
        }
        val completion = DebeziumEngine.CompletionCallback { success, message, error ->
            if (!success) {
                engineFailure.compareAndSet(
                    null,
                    error ?: HDataException(message?.takeIf { it.isNotBlank() } ?: "Debezium 引擎异常结束")
                )
            }
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
        val engineFailure = checkNotNull(failure) { "Debezium 引擎未初始化" }
        val limit = config.maxRecords
        // 入队端已保证最多只有 max_records 条有效记录；这里仍按实际输出计数，防止未来更换
        // consumer 实现时破坏上限不变量。
        var count = 0L
        while (true) {
            val row = rows.poll(200, TimeUnit.MILLISECONDS)
            if (row != null) {
                out.output(row)
                count++
                check(limit == null || count <= limit) { "Debezium 输出超过 max_records=$limit" }
                continue
            }
            if (done.get()) break
        }
        // 引擎自己结束了（有界快照），把队列里剩下的收干净，同样不越过上限
        while (true) {
            val rest = rows.poll() ?: break
            out.output(rest)
            count++
            if (limit != null && count >= limit) {
                return
            }
        }
        engineFailure.get()?.let { throw HDataException("Debezium 引擎执行失败", it) }
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
                // close()/stop() 会先通知运行循环结束，并在必要时自行中断阻塞的 poll；不要提前手动
                // interrupt，否则可能切断尚未完成的 offset flush。
                engine?.close()
            } catch (_: Throwable) {
            }
            try {
                thread?.join(5000)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }
}
