package me.jayer.hdata.elasticsearch6

import org.apache.http.HttpHost
import java.net.URI

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
