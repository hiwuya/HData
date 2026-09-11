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
 * The source DoFn that does change capture with the Debezium embedded engine.
 *
 * - `@Setup` builds and starts the [EmbeddedEngine] (running blocking on a dedicated thread); change events are
 *   converted into [Row]s by a Consumer and put into a blocking queue;
 * - `@ProcessElement` loops on the trigger element, taking rows from the queue and `output`ing them, until the engine
 *   finishes (bounded snapshot) or `max_records` is reached;
 * - `max_records` is counted precisely in the engine's consume callback; after the Nth valid record is enqueued,
 *   Debezium's own [StopEngineException] ends the engine normally, keeping the final offset commit and connector
 *   shutdown in native order;
 * - `@Teardown` only closes the engine as a fallback for paths like job cancellation and exceptions.
 *
 * The engine, thread, and queue are all marked `@Transient` and do not participate in serialization; connection-related
 * objects are built only on the worker via `@Setup`.
 */
class DebeziumReadFn(
    private val config: DebeziumReadConfig,
) : DoFn<String, Row>() {

    @Transient private var engine: EmbeddedEngine? = null
    @Transient private var thread: Thread? = null
    // The queue and stop flag are both nullable: a @Transient field is null after deserialization (property
    // initializers do not re-run), and @Teardown is still invoked even when @Setup fails. Declaring them non-null (or
    // lateinit) would throw NPE / UninitializedPropertyAccessException during cleanup, masking the real failure cause.
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
                    // Let the EmbeddedEngine exit normally from its own handler path; interrupting then closing it
                    // externally would break an in-progress offset flush, and the subsequent flush in finally would
                    // trigger a beginFlush re-entry error.
                    throw StopEngineException("reached max_records=${config.maxRecords}")
                }
            }
        }
        val completion = DebeziumEngine.CompletionCallback { success, message, error ->
            if (!success) {
                engineFailure.compareAndSet(
                    null,
                    error ?: HDataException(message?.takeIf { it.isNotBlank() } ?: "Debezium engine ended abnormally")
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
        val rows = checkNotNull(queue) { "Debezium engine is not initialized" }
        val done = checkNotNull(stopped) { "Debezium engine is not initialized" }
        val engineFailure = checkNotNull(failure) { "Debezium engine is not initialized" }
        val limit = config.maxRecords
        // The enqueue side already guarantees at most max_records valid records; we still count actual outputs here to
        // guard the upper-bound invariant against a future change of consumer implementation.
        var count = 0L
        while (true) {
            val row = rows.poll(200, TimeUnit.MILLISECONDS)
            if (row != null) {
                out.output(row)
                count++
                check(limit == null || count <= limit) { "Debezium output exceeded max_records=$limit" }
                continue
            }
            if (done.get()) break
        }
        // The engine finished on its own (bounded snapshot); drain what remains in the queue, likewise without exceeding the limit.
        while (true) {
            val rest = rows.poll() ?: break
            out.output(rest)
            count++
            if (limit != null && count >= limit) {
                return
            }
        }
        engineFailure.get()?.let { throw HDataException("Debezium engine execution failed", it) }
    }

    @Teardown
    fun teardown() {
        stopEngine()
    }

    @Synchronized
    private fun stopEngine() {
        // When @Setup has not run yet (or failed outright), stopped is null and there is no engine to clean up.
        if (stopped?.compareAndSet(false, true) == true) {
            try {
                // close()/stop() first notifies the run loop to end, and interrupts a blocked poll itself if needed;
                // do not interrupt manually in advance, otherwise an unfinished offset flush could be cut off.
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
