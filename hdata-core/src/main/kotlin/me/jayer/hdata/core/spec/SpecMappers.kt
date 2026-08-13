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
 * pipeline 文件的 Jackson mapper 工厂。
 *
 * pipeline 文件只支持 YAML（对齐 Beam YAML 规范）。解析结果是格式无关的
 * [tools.jackson.databind.JsonNode]，连接器不感知文件格式。
 *
 * - `snake_case` 命名策略：配置键沿用 Beam YAML 的写法（`fetch_size`），Kotlin 侧仍是驼峰。
 * - 开启 `FAIL_ON_UNKNOWN_PROPERTIES`：配置写错单词时直接报错，而不是静默丢弃。
 *
 * @author wuya
 * @date 2022-08-30
 */
object SpecMappers {

    /**
     * 用于把语法树绑定到连接器的配置类。
     *
     * 绑定只作用于语法树，与文件格式无关，所以这里用最轻的 [JsonMapper]。
     */
    val CONFIG: ObjectMapper = build(JsonMapper.builder())

    val YAML: ObjectMapper by lazy { build(YAMLMapper.builder()) }

    val supportedExtensions: Set<String> = setOf("yaml", "yml")

    fun forFile(file: File): ObjectMapper {
        val extension = file.extension.lowercase()
        if (extension !in supportedExtensions) {
            throw HDataException(
                "不支持的 pipeline 文件格式: .$extension，只支持 ${supportedExtensions.joinToString("/") { ".$it" }}"
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
