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
    val hosts = connectionUri.split(",").map { HttpHost.create(it.trim()) }.toTypedArray()
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
