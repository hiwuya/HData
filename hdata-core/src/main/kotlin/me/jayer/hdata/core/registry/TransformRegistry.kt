package me.jayer.hdata.core.registry

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider
import org.slf4j.LoggerFactory
import java.util.ServiceConfigurationError
import java.util.ServiceLoader

/**
 * `type` -> [TransformProvider] 的注册表。
 *
 * 两个来源，HData 自己的 provider 优先：
 * 1. `META-INF/services/me.jayer.hdata.core.spi.TransformProvider`
 * 2. classpath 上的 Beam 原生 [SchemaTransformProvider]（按 URN 注册，见 [BeamSchemaTransformAdapter]）
 *
 * @author wuya
 * @date 2022-08-30
 */
class TransformRegistry private constructor(private val providers: Map<String, TransformProvider>) {

    val identifiers: Set<String> get() = providers.keys

    fun find(type: String): TransformProvider? = providers[type]

    fun get(type: String): TransformProvider = find(type) ?: throw HDataException(
        "未知的 transform 类型: $type${suggestion(type)}\n已注册的类型: ${
            identifiers.filterNot { it.startsWith("beam:") }.sorted().joinToString(", ")
        }"
    )

    private fun suggestion(type: String): String {
        val candidates = identifiers.filter { it.equals(type, ignoreCase = true) || it.contains(type, ignoreCase = true) }
        return if (candidates.isEmpty()) "" else "，是否想用: ${candidates.sorted().joinToString(", ")}?"
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransformRegistry::class.java)

        fun discover(
            classLoader: ClassLoader = Thread.currentThread().contextClassLoader
                ?: TransformRegistry::class.java.classLoader,
        ): TransformRegistry {
            val providers = linkedMapOf<String, TransformProvider>()
            load(SchemaTransformProvider::class.java, classLoader).forEach { beamProvider ->
                register(providers, BeamSchemaTransformAdapter(beamProvider))
            }
            // 后注册，同名时覆盖 Beam 原生实现
            load(TransformProvider::class.java, classLoader).forEach { provider ->
                register(providers, provider)
            }
            LOGGER.debug("Discovered {} transform providers", providers.size)
            return TransformRegistry(providers)
        }

        private fun <T> load(type: Class<T>, classLoader: ClassLoader): List<T> {
            val loaded = mutableListOf<T>()
            val iterator = ServiceLoader.load(type, classLoader).iterator()
            while (true) {
                // 单个 provider 缺依赖时不应该拖垮整个注册表
                val hasNext = try {
                    iterator.hasNext()
                } catch (e: ServiceConfigurationError) {
                    LOGGER.warn("Stopped loading {} implementations: {}", type.simpleName, e.message)
                    break
                }
                if (!hasNext) break
                try {
                    loaded.add(iterator.next())
                } catch (e: ServiceConfigurationError) {
                    LOGGER.warn("Skipped an unloadable {} implementation: {}", type.simpleName, e.message)
                }
            }
            return loaded
        }

        private fun register(providers: MutableMap<String, TransformProvider>, provider: TransformProvider) {
            val identifier = provider.identifier()
            val previous = providers.put(identifier, provider)
            if (previous != null) {
                LOGGER.warn(
                    "Transform type[{}] is provided by both {} and {}, the latter wins",
                    identifier, previous.javaClass.name, provider.javaClass.name,
                )
            }
        }
    }
}
