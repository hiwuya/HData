package me.jayer.hdata.core

import me.jayer.hdata.core.config.HDataOptions
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.graph.PipelineGraph
import me.jayer.hdata.core.graph.PipelineGraphBuilder
import me.jayer.hdata.core.registry.TransformRegistry
import me.jayer.hdata.core.plugin.PluginManager
import me.jayer.hdata.core.spec.PipelineSpec
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.ExecutionSpec
import me.jayer.hdata.core.spi.SourceMode
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.options.FileStagingOptions
import org.slf4j.LoggerFactory
import tools.jackson.databind.JsonNode
import java.io.File
import kotlin.system.exitProcess

/**
 * HData's program entry point and embeddable API.
 *
 * Responsibilities are kept as thin as possible: parse the command line -> load the pipeline file
 * -> hand it to [PipelineGraphBuilder] for graph construction -> submit. Connector discovery,
 * config binding, and DAG assembly all live in their own modules, so this class holds no
 * connector-specific knowledge.
 *
 * @author wuya
 * @date 2022-08-30
 */
class HData(
    private val spec: PipelineSpec,
    private val registry: TransformRegistry = TransformRegistry.discover(),
) {

    /** Only builds the graph without submitting it; returns the constructed Beam pipeline and the graph description. */
    fun build(options: PipelineOptions): Pair<Pipeline, PipelineGraph> {
        configureExecution(options)
        val pipeline = Pipeline.create(options)
        val graph = PipelineGraphBuilder(registry).build(pipeline, spec.pipeline)
        return pipeline to graph
    }

    fun run(options: PipelineOptions): PipelineResult {
        val (pipeline, graph) = build(options)
        LOGGER.info("Pipeline graph:\n{}", graph.describe())
        logUndeclaredGraphMetadata(graph)
        return pipeline.run()
    }

    private fun logUndeclaredGraphMetadata(graph: PipelineGraph) {
        val undeclaredDelivery = graph.undeclaredDeliveryCapabilities()
        if (undeclaredDelivery.isNotEmpty()) {
            LOGGER.info(
                "No declared delivery contract (crash/restart behavior) for: {}. See docs/connectors.md for " +
                    "these connectors' documented behavior in prose.",
                undeclaredDelivery.joinToString(", ") { "${it.name} [${it.type}]" },
            )
        }
        val undeclaredTier = graph.undeclaredSupportTier()
        if (undeclaredTier.isNotEmpty()) {
            LOGGER.info(
                "No declared support tier (qualified/experimental/logic-tested-only) for: {}. Not yet " +
                    "classified is not the same as untested; see docs/MATURITY_ASSESSMENT.md.",
                undeclaredTier.joinToString(", ") { "${it.name} [${it.type}]" },
            )
        }
    }

    private fun configureExecution(options: PipelineOptions) {
        val sourceModes = PipelineGraphBuilder(registry).sourceModes(spec.pipeline)
        val hasUnboundedSource = SourceMode.UNBOUNDED in sourceModes
        val requested = spec.execution.resolvedMode()
        if (requested == ExecutionSpec.Mode.BATCH && hasUnboundedSource) {
            throw HDataException("execution.mode=batch cannot contain an unbounded source; use execution.mode=streaming or bound the source configuration")
        }
        val streaming = when (requested) {
            ExecutionSpec.Mode.AUTO -> hasUnboundedSource
            ExecutionSpec.Mode.BATCH -> false
            ExecutionSpec.Mode.STREAMING -> true
        }
        options.`as`(StreamingOptions::class.java).isStreaming = streaming
        LOGGER.info("Execution mode: {} (unbounded sources: {})", if (streaming) "streaming" else "batch", hasUnboundedSource)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(HData::class.java)

        private const val USAGE = "Usage: HData --pipeline=<pipeline file> [--runner=...] [--dryRun] [other Beam PipelineOptions]"

        @JvmStatic
        fun main(args: Array<String>) {
            PipelineOptionsFactory.register(HDataOptions::class.java)
            try {
                exitProcess(execute(args))
            } catch (e: HDataException) {
                LOGGER.error(e.message)
                exitProcess(1)
            } catch (e: Exception) {
                LOGGER.error("Job failed", e)
                exitProcess(1)
            }
        }

        private fun execute(args: Array<String>): Int {
            // The first parse pass only serves to obtain the pipeline file path; at this point we do not yet know which options are declared in the file
            val bootstrap = PipelineOptionsFactory.fromArgs(*args).withoutStrictParsing().`as`(HDataOptions::class.java)
            val path = bootstrap.getPipeline()
            if (path.isNullOrBlank()) {
                LOGGER.error("Missing --pipeline argument\n{}", USAGE)
                return 1
            }

            val spec = PipelineSpecLoader.load(File(path))
            val options = PipelineOptionsFactory.fromArgs(*mergeOptionArgs(spec, args))
                .withValidation()
                .`as`(HDataOptions::class.java)

            val registry = TransformRegistry.discover(pluginDirectories = PluginManager.parseDirectories(options.getPluginDirectories()))
            stagePluginArtifacts(options, registry.pluginArtifacts)
            val hdata = HData(spec, registry)
            if (options.getDryRun()) {
                val (_, graph) = hdata.build(options)
                LOGGER.info("Pipeline graph (dry run):\n{}", graph.describe())
                hdata.logUndeclaredGraphMetadata(graph)
                return 0
            }

            val result = hdata.run(options)
            if (!options.getWaitUntilFinish()) {
                LOGGER.info("Job submitted: state={}", result.state)
                return 0
            }
            val state = result.waitUntilFinish()
            LOGGER.info("Job finished: state={}", state)
            return if (state == PipelineResult.State.DONE) 0 else 1
        }

        /**
         * Merge the `options:` from the pipeline file with the command-line arguments into one Beam argument set.
         *
         * Options from the file are only defaults: once an option of the same name appears on the command line,
         * the file's copy is no longer passed down. We cannot simply rely on "command line comes later" to
         * override — Beam throws `expected one element but was: <DirectRunner, FlinkRunner>` on a duplicate
         * `--runner` instead of taking the latter.
         */
        internal fun mergeOptionArgs(spec: PipelineSpec, args: Array<String>): Array<String> {
            val fromCommandLine = args
                .filter { it.startsWith("--") }
                .map { it.removePrefix("--").substringBefore('=') }
                .toSet()
            val overridden = spec.options.keys.filter { it in fromCommandLine }
            if (overridden.isNotEmpty()) {
                LOGGER.info("Command line overrides options from the pipeline file: {}", overridden)
            }
            val fromFile = spec.options
                .filterKeys { it !in fromCommandLine }
                .map { (key, value) -> "--$key=${render(value)}" }
            return (fromFile + args).toTypedArray()
        }

        private fun render(value: JsonNode): String =
            if (value.isArray) value.joinToString(",") { it.asString() } else value.asString()

        /**
         * Beam runners use `filesToStage` to distribute user artifacts. Staging every JAR in a plugin
         * directory gives Flink and Spark workers the same bytes as the launcher; the runner remains
         * responsible for placing those JARs on its worker class path.
         */
        internal fun stagePluginArtifacts(options: PipelineOptions, artifacts: List<String>) {
            if (artifacts.isEmpty()) return
            val staging = options.`as`(FileStagingOptions::class.java)
            val merged = (staging.filesToStage.orEmpty() + artifacts).distinct()
            staging.filesToStage = merged
            LOGGER.info("Staging {} plugin artifact(s) for Beam workers", artifacts.size)
        }
    }
}
