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
import java.io.Serializable
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
 * 可序列化的 `ElasticsearchClient` 工厂，只为测试留的注入点；生产路径为 null。
 *
 * 为什么不直接注入一个假 client：`ElasticsearchClient` 是类，既不能用 `Proxy` 伪造，
 * 继承出来的子类又因为没有可访问的无参构造器而**没法被 Java 序列化**——
 * DirectRunner 序列化 DoFn 时会报 `no valid constructor`。
 * 工厂本身可序列化，反序列化之后在 worker 里再造假的客户端，绕开了这个限制
 * （`hdata-neo4j` 的 DriverFactory 是同一个套路）。
 */
fun interface EsClientFactory : Serializable {
    fun create(): ElasticsearchClient
}

/** API Key 与 Basic 是互斥的认证来源，避免一个配置被默认请求头静默盖过另一个。 */
internal fun validateEsAuthentication(apiKey: String, username: String, password: String) {
    require(apiKey.isEmpty() || apiKey.isNotBlank()) { "api_key 不能为空白字符串" }
    require(username.isEmpty() || username.isNotBlank()) { "username 不能为空白字符串" }
    require(password.isBlank() || username.isNotBlank()) { "配置 password 时必须同时配置 username" }
    require(apiKey.isBlank() || username.isBlank() && password.isBlank()) {
        "api_key 与 username/password 不能同时配置，请只选择一种认证方式"
    }
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
