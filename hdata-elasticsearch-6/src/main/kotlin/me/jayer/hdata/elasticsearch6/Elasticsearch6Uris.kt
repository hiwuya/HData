package me.jayer.hdata.elasticsearch6

import org.apache.http.HttpHost
import org.elasticsearch.client.RestHighLevelClient
import java.io.Serializable
import java.net.URI

/**
 * A serializable `RestHighLevelClient` factory, an injection point kept solely for testing; null on the production
 * path.
 *
 * Why not inject a fake client directly: `RestHighLevelClient`'s `bulk` / `search` are final methods, which can only be
 * stubbed via the inline mock maker, and the mock class it generates has no accessible no-arg constructor, so it
 * **cannot be Java-serialized** (DirectRunner reports `no valid constructor` when dispatching the DoFn).
 * The factory itself is serializable, and after deserialization the fake client is rebuilt on the worker, which sidesteps this limitation.
 */
fun interface Es6ClientFactory : Serializable {
    fun create(): RestHighLevelClient
}

/** ES REST node parsing shared by config validation and worker client construction. */
internal fun parseElasticsearch6Hosts(nodes: List<String>): Array<HttpHost> {
    require(nodes.isNotEmpty() && nodes.none(String::isBlank)) {
        "connection_uri must be a comma-separated list of valid HTTP(S) nodes"
    }
    return nodes.map { node ->
        val uri = runCatching { URI(node) }
            .getOrElse { throw IllegalArgumentException("connection_uri contains an invalid node: $node", it) }
        require(uri.scheme.equals("http", ignoreCase = true) ||
            uri.scheme.equals("https", ignoreCase = true)) {
            "connection_uri nodes only support http/https: $node"
        }
        require(!uri.host.isNullOrBlank()) { "connection_uri contains an invalid node: $node" }
        val host = runCatching { HttpHost.create(node) }
            .getOrElse { throw IllegalArgumentException("connection_uri contains an invalid node: $node", it) }
        host
    }.toTypedArray()
}
