package me.jayer.hdata.core.registry

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.plugin.PluginManager
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider
import org.slf4j.LoggerFactory
import java.util.ServiceConfigurationError
import java.util.ServiceLoader
import java.nio.file.Path

/**
 * The `type` -> [TransformProvider] registry.
 *
 * Two sources, with HData's own providers taking precedence:
 * 1. `META-INF/services/me.jayer.hdata.core.spi.TransformProvider`
 * 2. Beam's native [SchemaTransformProvider] on the classpath (registered by URN, see [BeamSchemaTransformAdapter])
 *
 * @author wuya
 * @date 2022-08-30
 */
class TransformRegistry private constructor(
    private val providers: Map<String, TransformProvider>,
    private val pluginManager: PluginManager?,
) : AutoCloseable {

    val identifiers: Set<String> get() = providers.keys

    fun find(type: String): TransformProvider? = providers[type]

    /** Plugin artifacts that must be staged to Beam workers with this registry. */
    val pluginArtifacts: List<String> get() = pluginManager?.artifactPaths.orEmpty()

    override fun close() = pluginManager?.close() ?: Unit

    fun get(type: String): TransformProvider = find(type) ?: throw HDataException(
        "Unknown transform type: $type${suggestion(type)}\nRegistered types: ${
            identifiers.filterNot { it.startsWith("beam:") }.sorted().joinToString(", ")
        }"
    )

    private fun suggestion(type: String): String {
        val candidates = identifiers.filter { it.equals(type, ignoreCase = true) || it.contains(type, ignoreCase = true) }
        return if (candidates.isEmpty()) "" else ", did you mean: ${candidates.sorted().joinToString(", ")}?"
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransformRegistry::class.java)

        fun discover(
            classLoader: ClassLoader = Thread.currentThread().contextClassLoader
                ?: TransformRegistry::class.java.classLoader,
            pluginDirectories: List<Path> = emptyList(),
        ): TransformRegistry {
            val providers = linkedMapOf<String, TransformProvider>()
            load(SchemaTransformProvider::class.java, classLoader).forEach { beamProvider ->
                register(providers, BeamSchemaTransformAdapter(beamProvider))
            }
            // Registered afterwards, so on a name clash it overrides Beam's native implementation
            load(TransformProvider::class.java, classLoader).forEach { provider ->
                register(providers, provider)
            }
            val plugins = PluginManager.discover(pluginDirectories, classLoader)
            try {
                plugins.providers.forEach { provider ->
                    if (provider.identifier() in providers) {
                        throw HDataException("Plugin provider [${provider.identifier()}] conflicts with an installed transform type")
                    }
                    register(providers, provider)
                }
            } catch (e: Exception) {
                plugins.close()
                throw e
            }
            LOGGER.debug("Discovered {} transform providers", providers.size)
            return TransformRegistry(providers, plugins)
        }

        private fun <T> load(type: Class<T>, classLoader: ClassLoader): List<T> {
            val loaded = mutableListOf<T>()
            val iterator = ServiceLoader.load(type, classLoader).iterator()
            while (true) {
                // A single provider missing dependencies should not drag down the entire registry
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
