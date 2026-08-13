package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.cfg.MapperBuilder
import tools.jackson.databind.json.JsonMapper
import tools.jackson.dataformat.toml.TomlMapper
import tools.jackson.dataformat.yaml.YAMLMapper
import tools.jackson.module.kotlin.KotlinModule
import java.io.File

/**
 * pipeline 文件的 Jackson mapper 工厂。
 *
 * 所有格式共享同一套绑定规则，因此 YAML / TOML / JSON 解析出的语法树完全等价，
 * 连接器拿到的只是格式无关的 [tools.jackson.databind.JsonNode]，不感知文件格式。
 *
 * - `snake_case` 命名策略：配置键沿用 Beam YAML 的写法（`fetch_size`），Kotlin 侧仍是驼峰。
 * - 开启 `FAIL_ON_UNKNOWN_PROPERTIES`：配置写错单词时直接报错，而不是静默丢弃。
 *
 * @author wuya
 * @date 2022-08-30
 */
object SpecMappers {

    /** 用于把语法树绑定到连接器的配置类，与文件格式无关；也是 JSON 格式的 mapper。 */
    val CONFIG: ObjectMapper = build(JsonMapper.builder())

    val YAML: ObjectMapper by lazy { build(YAMLMapper.builder()) }

    val TOML: ObjectMapper by lazy { build(TomlMapper.builder()) }

    private val BY_EXTENSION: Map<String, () -> ObjectMapper> =
        mapOf("yaml" to { YAML }, "yml" to { YAML }, "toml" to { TOML }, "json" to { CONFIG })

    val supportedExtensions: Set<String> get() = BY_EXTENSION.keys

    fun forFile(file: File): ObjectMapper {
        val extension = file.extension.lowercase()
        val mapper = BY_EXTENSION[extension]
            ?: throw HDataException("不支持的 pipeline 文件格式: .$extension，支持 ${supportedExtensions.joinToString("/") { ".$it" }}")
        return mapper()
    }

    private fun <M : ObjectMapper, B : MapperBuilder<M, B>> build(builder: MapperBuilder<M, B>): M =
        builder
            .addModule(KotlinModule.Builder().build())
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()
}
