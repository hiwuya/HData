package me.jayer.hdata.core.plugin

import java.net.URL
import java.net.URLClassLoader

/**
 * A child-first loader for a connector's private client libraries.
 *
 * Beam, the HData SPI, Kotlin and logging are always resolved by the host. They appear in pipeline
 * objects and therefore must have one class identity in the launcher and every worker.
 */
internal class PluginClassLoader(urls: Array<URL>, parent: ClassLoader) : URLClassLoader(urls, parent) {

    override fun loadClass(name: String, resolve: Boolean): Class<*> = synchronized(getClassLoadingLock(name)) {
        findLoadedClass(name)?.let { return it }
        if (isParentFirst(name)) return super.loadClass(name, resolve)

        val loaded = try {
            findClass(name)
        } catch (_: ClassNotFoundException) {
            parent.loadClass(name)
        }
        if (resolve) resolveClass(loaded)
        loaded
    }

    private fun isParentFirst(name: String): Boolean = PARENT_FIRST_PREFIXES.any(name::startsWith)

    private companion object {
        val PARENT_FIRST_PREFIXES = listOf(
            "java.", "javax.", "jdk.", "kotlin.",
            "org.apache.beam.", "org.slf4j.", "org.apache.logging.",
            // hdata-plugin-api retains the established package names for source compatibility.
            "me.jayer.hdata.core.spi.", "me.jayer.hdata.core.spec.ErrorHandlingSpec", "me.jayer.hdata.core.exception.",
        )
    }
}
