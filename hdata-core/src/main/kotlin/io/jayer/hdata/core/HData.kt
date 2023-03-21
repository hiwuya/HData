package io.jayer.hdata.core

import io.jayer.hdata.core.config.HDataJobConfig
import io.jayer.hdata.core.config.HDataOptions
import io.jayer.hdata.core.spi.*
import io.jayer.hdata.core.util.ObjectMappers
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*
import kotlin.system.exitProcess

/**
 * @author wuya
 * @date 2022-08-30
 */
class HData(
    private val sources: List<StructuredSource>,
    private val transforms: List<StructuredTransform>,
    private val sinks: List<StructuredSink>
) {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(HData::class.java)

        private fun loadIOProvider(name: String): StructuredIOProvider? {
            return ServiceLoader.load(StructuredIOProvider::class.java).firstOrNull { it.identifier() == name }
        }

        private fun loadSource(name: String, config: Map<String, Any>): StructuredSource {
            val provider = loadIOProvider(name)
            requireNotNull(provider) { "Source not exists: $name" }
            return provider.createSource(config)
        }

        private fun loadSink(name: String, config: Map<String, Any>): StructuredSink {
            val provider = loadIOProvider(name)
            requireNotNull(provider) { "Sink not exists: $name" }
            return provider.createSink(config)
        }

        private fun loadTransformProvider(name: String): StructuredTransformProvider? {
            return ServiceLoader.load(StructuredTransformProvider::class.java).firstOrNull { it.identifier() == name }
        }

        private fun loadTransform(name: String, config: Map<String, Any>): StructuredTransform {
            val provider = loadTransformProvider(name)
            requireNotNull(provider) { "Transform not exists: $name" }
            return provider.createTransform(config)
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(HDataOptions::class.java)
            val config = File(options.getConfig())
            if (!config.exists()) {
                LOGGER.error("Config file not exist: {}", config.path)
                exitProcess(1)
            }

            val objectMapper = ObjectMappers.getTomlObjectMapper()
            val jobConfig = try {
                objectMapper.readValue(config, HDataJobConfig::class.java)
            } catch (e: Exception) {
                LOGGER.error("Invalid config file", e)
                exitProcess(1)
            }

            val sources = jobConfig.sources.filter { it.isNotEmpty() }
            val transforms = jobConfig.transforms.filter { it.isNotEmpty() }
            val sinks = jobConfig.sinks.filter { it.isNotEmpty() }
            require(sources.isNotEmpty()) { "Sources should not be empty" }
            require(sinks.isNotEmpty()) { "Sinks should not be empty" }

            val hdata = HData(
                sources.map { conf ->
                    val sourceType = conf["type"] as String?
                    require(!sourceType.isNullOrBlank()) { "Source type is not specified" }
                    loadSource(sourceType, conf.filter { it.key != "type" })
                },
                transforms.map { conf ->
                    val transformType = conf["type"] as String?
                    require(!transformType.isNullOrBlank()) { "Transform type is not specified" }
                    loadTransform(transformType, conf.filter { it.key != "type" })
                },
                sinks.map { conf ->
                    val sinkType = conf["type"] as String?
                    require(!sinkType.isNullOrBlank()) { "Sink type is not specified" }
                    loadSink(sinkType, conf.filter { it.key != "type" })
                }
            )
            hdata.start(options)
        }
    }

    fun start(options: PipelineOptions) {
        val pipeline = Pipeline.create(options)
        for (source in sources) {
            var input = pipeline.apply(source)
            transforms.forEach { input = input.apply(it) }
            sinks.forEach { input.apply(it) }
        }

        pipeline.run().waitUntilFinish()
    }
}