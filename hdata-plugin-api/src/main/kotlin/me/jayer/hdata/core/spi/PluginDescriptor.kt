package me.jayer.hdata.core.spi

/**
 * Metadata required to load one isolated connector plugin.
 *
 * The descriptor lives at [RESOURCE_PATH] inside the plugin's main JAR. It deliberately names one
 * [providerClass] instead of relying on the host application's service resources: a plugin loader
 * must never accidentally discover providers packaged by the host or by another plugin.
 */
data class PluginDescriptor(
    val id: String,
    val apiVersion: String,
    val providerClass: String,
) {
    companion object {
        const val API_VERSION = "1"
        const val RESOURCE_PATH = "META-INF/hdata-plugin.properties"
    }
}
