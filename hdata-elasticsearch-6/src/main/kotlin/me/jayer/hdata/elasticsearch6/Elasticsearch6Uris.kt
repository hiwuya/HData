package me.jayer.hdata.elasticsearch6

import org.apache.http.HttpHost
import org.elasticsearch.client.RestHighLevelClient
import java.io.Serializable
import java.net.URI

/**
 * 可序列化的 `RestHighLevelClient` 工厂，只为测试留的注入点；生产路径为 null。
 *
 * 为什么不直接注入一个假 client：`RestHighLevelClient` 的 `bulk` / `search` 都是 final 方法，
 * 只能靠 inline mock maker 打桩，而它生成的 mock 类没有可访问的无参构造器，**没法被 Java 序列化**
 * （DirectRunner 下发 DoFn 时会报 `no valid constructor`）。
 * 工厂本身可序列化，反序列化之后在 worker 里再造假的客户端，绕开了这个限制。
 */
fun interface Es6ClientFactory : Serializable {
    fun create(): RestHighLevelClient
}

/** 配置校验和 worker 客户端构造共用的 ES REST 节点解析。 */
internal fun parseElasticsearch6Hosts(nodes: List<String>): Array<HttpHost> {
    require(nodes.isNotEmpty() && nodes.none(String::isBlank)) {
        "connection_uri 必须是逗号分隔的有效 HTTP(S) 节点"
    }
    return nodes.map { node ->
        val uri = runCatching { URI(node) }
            .getOrElse { throw IllegalArgumentException("connection_uri 包含无效节点: $node", it) }
        require(uri.scheme.equals("http", ignoreCase = true) ||
            uri.scheme.equals("https", ignoreCase = true)) {
            "connection_uri 节点只支持 http/https: $node"
        }
        require(!uri.host.isNullOrBlank()) { "connection_uri 包含无效节点: $node" }
        val host = runCatching { HttpHost.create(node) }
            .getOrElse { throw IllegalArgumentException("connection_uri 包含无效节点: $node", it) }
        host
    }.toTypedArray()
}
