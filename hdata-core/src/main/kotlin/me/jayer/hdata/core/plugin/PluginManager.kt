package me.jayer.hdata.core.plugin

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.PluginDescriptor
import me.jayer.hdata.core.spi.TransformProvider
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.jar.JarFile
import kotlin.io.path.extension
import kotlin.io.path.name

/** Discovers and retains isolated connector plugin loaders for the life of a submitted pipeline. */
class PluginManager private constructor(private val plugins: List<LoadedPlugin>) : AutoCloseable {

    val providers: List<TransformProvider> = plugins.map { it.provider }

    /** Every JAR required by the plugin; runners must stage this exact list to workers. */
    val artifactPaths: List<String> = plugins.flatMap { it.artifacts }.map(Path::toString)

    override fun close() = plugins.asReversed().forEach { it.classLoader.close() }

    companion object {
        fun discover(directories: List<Path>, parent: ClassLoader = PluginManager::class.java.classLoader): PluginManager {
            val plugins = directories.map { directory -> load(directory.toAbsolutePath().normalize(), parent) }
            val duplicateIds = plugins.groupingBy { it.descriptor.id }.eachCount().filterValues { it > 1 }.keys
            if (duplicateIds.isNotEmpty()) {
                plugins.asReversed().forEach { it.classLoader.close() }
                throw HDataException("Duplicate plugin ids: ${duplicateIds.sorted().joinToString(", ")}")
            }
            return PluginManager(plugins)
        }

        fun parseDirectories(value: String?): List<Path> = value
            ?.split(',')
            ?.map(String::trim)
            ?.filter(String::isNotEmpty)
            ?.map(Path::of)
            ?: emptyList()

        private fun load(directory: Path, parent: ClassLoader): LoadedPlugin {
            if (!Files.isDirectory(directory)) throw HDataException("Plugin directory does not exist or is not a directory: $directory")
            val jars = Files.list(directory).use { files ->
                files.filter { Files.isRegularFile(it) && it.extension.equals("jar", ignoreCase = true) }
                    .sorted()
                    .toList()
            }
            if (jars.isEmpty()) throw HDataException("Plugin directory contains no JAR files: $directory")
            val descriptorJar = jars.singleOrNull { jar -> JarFile(jar.toFile()).use { it.getJarEntry(PluginDescriptor.RESOURCE_PATH) != null } }
                ?: throw HDataException("Plugin directory must contain exactly one JAR with ${PluginDescriptor.RESOURCE_PATH}: $directory")
            val descriptor = JarFile(descriptorJar.toFile()).use { jar ->
                val properties = Properties().also { values ->
                    jar.getInputStream(jar.getJarEntry(PluginDescriptor.RESOURCE_PATH)).use(values::load)
                }
                PluginDescriptor(
                    id = required(properties, "id", descriptorJar),
                    apiVersion = required(properties, "api_version", descriptorJar),
                    providerClass = required(properties, "provider_class", descriptorJar),
                )
            }
            if (descriptor.apiVersion != PluginDescriptor.API_VERSION) {
                throw HDataException("Plugin [${descriptor.id}] requires API ${descriptor.apiVersion}, host provides ${PluginDescriptor.API_VERSION}")
            }

            val loader = PluginClassLoader(jars.map { it.toUri().toURL() }.toTypedArray(), parent)
            return try {
                val type = loader.loadClass(descriptor.providerClass)
                if (!TransformProvider::class.java.isAssignableFrom(type)) {
                    throw HDataException("Plugin [${descriptor.id}] provider ${descriptor.providerClass} does not implement TransformProvider")
                }
                @Suppress("UNCHECKED_CAST")
                val provider = type.getDeclaredConstructor().newInstance() as TransformProvider
                LoadedPlugin(descriptor, provider, loader, jars)
            } catch (e: HDataException) {
                loader.close()
                throw e
            } catch (e: Exception) {
                loader.close()
                throw HDataException("Cannot load plugin [${descriptor.id}] from $directory: ${e.message}", e)
            }
        }

        private fun required(properties: Properties, key: String, jar: Path): String =
            properties.getProperty(key)?.trim()?.takeIf(String::isNotEmpty)
                ?: throw HDataException("Plugin descriptor in ${jar.name} is missing [$key]")
    }

    private data class LoadedPlugin(
        val descriptor: PluginDescriptor,
        val provider: TransformProvider,
        val classLoader: URLClassLoader,
        val artifacts: List<Path>,
    )
}
