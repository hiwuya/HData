package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.cfg.MapperBuilder
import tools.jackson.databind.json.JsonMapper
import tools.jackson.dataformat.yaml.YAMLMapper
import tools.jackson.module.kotlin.KotlinModule
import java.io.File

/**
 * The Jackson mapper factory for pipeline files.
 *
 * Pipeline files only support YAML (aligned with the Beam YAML spec). The parse result is a
 * format-agnostic [tools.jackson.databind.JsonNode], so connectors are unaware of the file format.
 *
 * - `snake_case` naming strategy: config keys follow Beam YAML's style (`fetch_size`), while the Kotlin side stays camelCase.
 * - `FAIL_ON_UNKNOWN_PROPERTIES` enabled: a misspelled config word errors out directly instead of being silently dropped.
 *
 * @author wuya
 * @date 2022-08-30
 */
object SpecMappers {

    /**
     * Used to bind the syntax tree to a connector's config class.
     *
     * Binding only operates on the syntax tree and is independent of the file format, so we use the lightest [JsonMapper] here.
     */
    val CONFIG: ObjectMapper = build(JsonMapper.builder())

    val YAML: ObjectMapper by lazy { build(YAMLMapper.builder()) }

    val supportedExtensions: Set<String> = setOf("yaml", "yml")

    fun forFile(file: File): ObjectMapper {
        val extension = file.extension.lowercase()
        if (extension !in supportedExtensions) {
            throw HDataException(
                "Unsupported pipeline file format: .$extension, only ${supportedExtensions.joinToString("/") { ".$it" }} are supported"
            )
        }
        return YAML
    }

    private fun <M : ObjectMapper, B : MapperBuilder<M, B>> build(builder: MapperBuilder<M, B>): M =
        builder
            .addModule(KotlinModule.Builder().build())
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()
}
