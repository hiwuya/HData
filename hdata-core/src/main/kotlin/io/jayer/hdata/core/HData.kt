package io.jayer.hdata.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.jayer.hdata.core.config.HDataOptions
import io.jayer.hdata.core.config.HDataYamlConfig
import io.jayer.hdata.core.spi.*
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

        fun loadIOProvider(name: String): StructuredIOProvider? {
            return ServiceLoader.load(StructuredIOProvider::class.java).firstOrNull { it.identifier() == name }
        }

        fun loadSource(name: String, config: Map<String, Any>): StructuredSource {
            val provider = loadIOProvider(name)
            requireNotNull(provider) { "Source not exists: $name" }
            return provider.createSource(config)
        }

        fun loadSink(name: String, config: Map<String, Any>): StructuredSink {
            val provider = loadIOProvider(name)
            requireNotNull(provider) { "Sink not exists: $name" }
            return provider.createSink(config)
        }

        fun loadTransformProvider(name: String): StructuredTransformProvider? {
            return ServiceLoader.load(StructuredTransformProvider::class.java).firstOrNull { it.identifier() == name }
        }

        fun loadTransform(name: String, config: Map<String, Any>): StructuredTransform {
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

            val objectMapper = ObjectMapper(YAMLFactory()).findAndRegisterModules()
            val yamlConfig = try {
                objectMapper.readValue(config, HDataYamlConfig::class.java)
            } catch (e: Exception) {
                LOGGER.error("Invalid config file", e)
                exitProcess(1)
            }

            require(yamlConfig.source.isNotEmpty()) { "Source should not be empty" }
            require(yamlConfig.sink.isNotEmpty()) { "Sink should not be empty" }

            val hdata = HData(
                yamlConfig.source.flatMap { it.map { (name, config) -> loadSource(name, config) } },
                yamlConfig.transform.flatMap { it.map { (name, config) -> loadTransform(name, config) } },
                yamlConfig.sink.flatMap { it.map { (name, config) -> loadSink(name, config) } }
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