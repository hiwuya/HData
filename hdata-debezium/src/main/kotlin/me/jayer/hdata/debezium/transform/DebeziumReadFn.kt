package me.jayer.hdata.debezium.transform

import io.debezium.embedded.EmbeddedEngine
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.StopEngineException
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.debezium.DebeziumReadConfig
import me.jayer.hdata.debezium.internal.DebeziumRecords
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.Row
import org.apache.kafka.connect.source.SourceRecord
import org.joda.time.Duration
import org.joda.time.Instant
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * The source DoFn that does change capture with the Debezium embedded engine.
 *
 * This is a splittable DoFn ([DoFn.UnboundedPerElement]): CDC is an inherently unbounded stream, and modeling it
 * that way (rather than the previous plain-DoFn "loop on one call until the engine stops" approach) buys two things
 * a bounded DoFn cannot give:
 *  - `@ProcessElement` returns [DoFn.ProcessContinuation.resume] every [PROCESS_TIME_BUDGET] instead of blocking a
 *    worker thread for the entire capture; the runner is free to schedule other bundles between resumes.
 *  - a real watermark (via [ManualWatermarkEstimator], advanced from each event's `ts_ms`), which a downstream
 *    windowed aggregation or a streaming runner (FlinkRunner in streaming mode) needs to make progress correctly —
 *    the previous bounded implementation had no watermark concept at all.
 *
 * The [OffsetRange] restriction does not correspond to a real Debezium offset (that is still owned and persisted by
 * Debezium's own offset-storage file, unchanged); it is a monotonically increasing "records emitted so far" counter
 * used only to give the Beam runner a checkpoint/resume and progress-reporting contract. When
 * [DebeziumReadConfig.maxRecords] is set, its upper bound naturally bounds the restriction so the read still
 * terminates like a snapshot; left unset, the upper bound is [Long.MAX_VALUE] and the read runs indefinitely, same
 * as CDC itself.
 *
 * - `@Setup` builds and starts the [EmbeddedEngine] (running blocking on a dedicated thread); change events are
 *   converted into [Row]s by a Consumer and put into a blocking queue;
 * - `max_records` is counted precisely in the engine's consume callback; after the Nth valid record is enqueued,
 *   Debezium's own [StopEngineException] ends the engine normally, keeping the final offset commit and connector
 *   shutdown in native order;
 * - `@Teardown` only closes the engine as a fallback for paths like job cancellation and exceptions.
 *
 * The engine, thread, and queue are all marked `@Transient` and do not participate in serialization; connection-related
 * objects are built only on the worker via `@Setup`.
 */
@DoFn.UnboundedPerElement
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

    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange = OffsetRange(0, config.maxRecords?.toLong() ?: Long.MAX_VALUE)

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = OffsetRangeTracker(restriction)

    @GetInitialWatermarkEstimatorState
    fun getInitialWatermarkEstimatorState(): Instant = BoundedWindow.TIMESTAMP_MIN_VALUE

    @NewWatermarkEstimator
    fun newWatermarkEstimator(
        @WatermarkEstimatorState state: Instant,
    ): WatermarkEstimators.Manual = WatermarkEstimators.Manual(state)

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
    fun processElement(
        @Element element: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        watermarkEstimator: ManualWatermarkEstimator<Instant>,
        out: OutputReceiver<Row>,
    ): ProcessContinuation {
        val rows = checkNotNull(queue) { "Debezium engine is not initialized" }
        val done = checkNotNull(stopped) { "Debezium engine is not initialized" }
        val engineFailure = checkNotNull(failure) { "Debezium engine is not initialized" }

        // Resume from wherever the last call (or a runner-driven split) left off — never from a field on this
        // instance, which is not guaranteed to survive between @ProcessElement invocations.
        var position = tracker.currentRestriction().from
        val deadline = System.currentTimeMillis() + PROCESS_TIME_BUDGET.millis

        while (System.currentTimeMillis() < deadline) {
            val row = rows.poll(POLL_INTERVAL.millis, TimeUnit.MILLISECONDS)
            if (row == null) {
                if (done.get() && rows.isEmpty()) {
                    engineFailure.get()?.let { throw HDataException("Debezium engine execution failed", it) }
                    return ProcessContinuation.stop()
                }
                continue
            }
            if (!tracker.tryClaim(position)) {
                return ProcessContinuation.stop()
            }
            val timestamp = extractTimestamp(row)
            out.outputWithTimestamp(row, timestamp)
            watermarkEstimator.setWatermark(timestamp)
            position++
        }

        // Ran out of time budget, not data: give the worker thread back to the runner and pick up again from
        // `position` (via tracker.currentRestriction().from) on the next call.
        if (done.get() && rows.isEmpty()) {
            engineFailure.get()?.let { throw HDataException("Debezium engine execution failed", it) }
            return ProcessContinuation.stop()
        }
        return ProcessContinuation.resume().withResumeDelay(RESUME_DELAY)
    }

    private fun extractTimestamp(row: Row): Instant {
        val tsMs = row.getInt64("ts_ms") ?: return Instant.now()
        return Instant(tsMs)
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

    companion object {
        /** How long a single @ProcessElement call polls the queue before yielding back to the runner. */
        private val PROCESS_TIME_BUDGET = Duration.millis(2000)

        /** Poll granularity while waiting for the next change event within one @ProcessElement call. */
        private val POLL_INTERVAL = Duration.millis(200)

        /** Hint to the runner for how soon to re-invoke @ProcessElement after a resume with no new data. */
        private val RESUME_DELAY = Duration.millis(100)
    }
}
