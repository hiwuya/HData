package me.jayer.hdata.core.spi

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.ErrorHandlingSpec
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import tools.jackson.databind.node.ObjectNode

/**
 * The config view handed to connectors.
 *
 * This intentionally only exposes the format-agnostic syntax tree, so connectors do not need to know
 * what the pipeline file looks like, and therefore will not depend directly on `TomlMapper` in their
 * implementation as they did before the refactor.
 *
 * `error_handling` has already been stripped by the framework and is exposed via [errorHandling], so
 * config classes can safely enable `FAIL_ON_UNKNOWN_PROPERTIES` to catch typos.
 *
 * @author wuya
 * @date 2022-08-30
 */
class TransformConfig(
    /** The transform's name in the DAG, used only for error localization. */
    val transformName: String,
    private val node: ObjectNode,
    val errorHandling: ErrorHandlingSpec? = null,
) {

    val raw: ObjectNode get() = node

    val isEmpty: Boolean get() = node.isEmpty

    /** Binds the config to the connector's own config class, matching keys by `snake_case`. */
    fun <T : Any> bind(type: Class<T>): T = try {
        CONFIG_MAPPER.treeToValue(node, type)
    } catch (e: Exception) {
        throw HDataException("transform[$transformName]'s config is invalid: ${e.message}", e)
    }

    override fun toString(): String = "TransformConfig(transform=$transformName, keys=${node.propertyNames().toList()})"

    private companion object {
        val CONFIG_MAPPER = JsonMapper.builder()
            .addModule(KotlinModule.Builder().build())
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()
    }
}
