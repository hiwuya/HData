package me.jayer.hdata.elasticsearch8

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.RestClient
import java.net.URI

/**
 * 用新的 Java client 构造 `ElasticsearchClient`（以及底层的 `RestClient`，用于关闭）。
 *
 * 优先用 API Key 或 用户名/密码 做认证；都不填则匿名连接。
 */
internal fun buildEsClient(
    connectionUri: String,
    apiKey: String,
    username: String,
    password: String,
): Pair<ElasticsearchClient, RestClient> {
    val hosts = parseEsHosts(connectionUri)
    val builder = RestClient.builder(*hosts)
    if (username.isNotBlank() || apiKey.isNotBlank()) {
        builder.setHttpClientConfigCallback { httpBuilder ->
            val creds = BasicCredentialsProvider()
            if (username.isNotBlank()) {
                creds.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(username, password))
            }
            httpBuilder.setDefaultCredentialsProvider(creds)
            if (apiKey.isNotBlank()) {
                httpBuilder.setDefaultHeaders(listOf(BasicHeader("Authorization", "ApiKey $apiKey")))
            }
            httpBuilder
        }
    }
    val restClient = builder.build()
    val transport = RestClientTransport(restClient, JacksonJsonpMapper(ObjectMapper()))
    return ElasticsearchClient(transport) to restClient
}

/**
 * 解析并校验逗号分隔的 REST 节点。配置校验与客户端构造共用这一入口，避免无效地址到 worker
 * 的 `@Setup` 阶段才暴露。
 */
internal fun parseEsHosts(connectionUri: String): Array<HttpHost> {
    val nodes = connectionUri.split(",".toRegex(), Int.MAX_VALUE).map(String::trim)
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
