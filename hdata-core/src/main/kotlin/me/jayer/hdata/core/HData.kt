package me.jayer.hdata.core

import me.jayer.hdata.core.config.HDataOptions
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.graph.PipelineGraph
import me.jayer.hdata.core.graph.PipelineGraphBuilder
import me.jayer.hdata.core.registry.TransformRegistry
import me.jayer.hdata.core.spec.PipelineSpec
import me.jayer.hdata.core.spec.PipelineSpecLoader
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.slf4j.LoggerFactory
import tools.jackson.databind.JsonNode
import java.io.File
import kotlin.system.exitProcess

/**
 * HData 的程序入口与可嵌入 API。
 *
 * 职责被压到最薄：解析命令行 -> 加载 pipeline 文件 -> 交给 [PipelineGraphBuilder] 构图 -> 提交。
 * 连接器的发现、配置绑定、DAG 拼装都在各自的模块里，这里不再有任何连接器相关的知识。
 *
 * @author wuya
 * @date 2022-08-30
 */
class HData(
    private val spec: PipelineSpec,
    private val registry: TransformRegistry = TransformRegistry.discover(),
) {

    /** 只构图不提交，返回构好的 Beam pipeline 与图的描述。 */
    fun build(options: PipelineOptions): Pair<Pipeline, PipelineGraph> {
        val pipeline = Pipeline.create(options)
        val graph = PipelineGraphBuilder(registry).build(pipeline, spec.pipeline)
        return pipeline to graph
    }

    fun run(options: PipelineOptions): PipelineResult {
        val (pipeline, graph) = build(options)
        LOGGER.info("Pipeline graph:\n{}", graph.describe())
        return pipeline.run()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(HData::class.java)

        private const val USAGE = "用法: HData --pipeline=<pipeline 文件> [--runner=...] [--dryRun] [其他 Beam PipelineOptions]"

        @JvmStatic
        fun main(args: Array<String>) {
            PipelineOptionsFactory.register(HDataOptions::class.java)
            try {
                exitProcess(execute(args))
            } catch (e: HDataException) {
                LOGGER.error(e.message)
                exitProcess(1)
            } catch (e: Exception) {
                LOGGER.error("作业失败", e)
                exitProcess(1)
            }
        }

        private fun execute(args: Array<String>): Int {
            // 第一遍解析只为拿到 pipeline 文件路径，此时还不知道文件里声明了哪些 options
            val bootstrap = PipelineOptionsFactory.fromArgs(*args).withoutStrictParsing().`as`(HDataOptions::class.java)
            val path = bootstrap.getPipeline()
            if (path.isNullOrBlank()) {
                LOGGER.error("缺少 --pipeline 参数\n{}", USAGE)
                return 1
            }

            val spec = PipelineSpecLoader.load(File(path))
            val options = PipelineOptionsFactory.fromArgs(*mergeOptionArgs(spec, args))
                .withValidation()
                .`as`(HDataOptions::class.java)

            val hdata = HData(spec)
            if (options.getDryRun()) {
                val (_, graph) = hdata.build(options)
                LOGGER.info("Pipeline graph (dry run):\n{}", graph.describe())
                return 0
            }

            val result = hdata.run(options)
            if (!options.getWaitUntilFinish()) {
                LOGGER.info("作业已提交: state={}", result.state)
                return 0
            }
            val state = result.waitUntilFinish()
            LOGGER.info("作业结束: state={}", state)
            return if (state == PipelineResult.State.DONE) 0 else 1
        }

        /**
         * 把 pipeline 文件里的 `options:` 与命令行参数合成一份 Beam 参数。
         *
         * 文件里的选项只当默认值：同名选项一旦出现在命令行上，就**不再**把文件里那份传下去。
         * 不能简单地靠"命令行放后面"来覆盖——Beam 见到重复的 `--runner` 会直接抛
         * `expected one element but was: <DirectRunner, FlinkRunner>`，而不是取后者。
         */
        internal fun mergeOptionArgs(spec: PipelineSpec, args: Array<String>): Array<String> {
            val fromCommandLine = args
                .filter { it.startsWith("--") }
                .map { it.removePrefix("--").substringBefore('=') }
                .toSet()
            val overridden = spec.options.keys.filter { it in fromCommandLine }
            if (overridden.isNotEmpty()) {
                LOGGER.info("命令行覆盖了 pipeline 文件里的选项: {}", overridden)
            }
            val fromFile = spec.options
                .filterKeys { it !in fromCommandLine }
                .map { (key, value) -> "--$key=${render(value)}" }
            return (fromFile + args).toTypedArray()
        }

        private fun render(value: JsonNode): String =
            if (value.isArray) value.joinToString(",") { it.asString() } else value.asString()
    }
}
