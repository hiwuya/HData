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
 * Creates an `ElasticsearchClient` and its closeable underlying `RestClient` with the Java client.
 * Authentication uses either an API key or a username and password; empty credentials use an anonymous connection.
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
 * Serializable `ElasticsearchClient` factory used only as a test injection point; production uses null.
 *
 * A client cannot be injected directly: it cannot be faked with `Proxy`, and subclasses lack an accessible
 * no-argument constructor for Java serialization. The factory is serializable and creates the test client on the worker.
 */
fun interface EsClientFactory : Serializable {
    fun create(): ElasticsearchClient
}

/** API-key and Basic authentication are mutually exclusive so request defaults cannot silently override either one. */
internal fun validateEsAuthentication(apiKey: String, username: String, password: String) {
    require(apiKey.isEmpty() || apiKey.isNotBlank()) { "api_key must not be whitespace" }
    require(username.isEmpty() || username.isNotBlank()) { "username must not be whitespace" }
    require(password.isBlank() || username.isNotBlank()) { "password requires username" }
    require(apiKey.isBlank() || username.isBlank() && password.isBlank()) {
        "api_key and username/password cannot be configured together; choose one authentication method"
    }
}

/**
 * Parses and validates comma-separated REST nodes for both configuration validation and client creation,
 * so invalid URLs fail before worker setup.
 */
internal fun parseEsHosts(connectionUri: String): Array<HttpHost> {
    val nodes = connectionUri.split(",".toRegex(), Int.MAX_VALUE).map(String::trim)
    require(nodes.isNotEmpty() && nodes.none(String::isBlank)) {
        "connection_uri must contain valid comma-separated HTTP(S) nodes"
    }
    return nodes.map { node ->
        val uri = runCatching { URI(node) }
            .getOrElse { throw IllegalArgumentException("connection_uri contains an invalid node: $node", it) }
        require(uri.scheme.equals("http", ignoreCase = true) ||
            uri.scheme.equals("https", ignoreCase = true)) {
            "connection_uri nodes support only http/https: $node"
        }
        require(!uri.host.isNullOrBlank()) { "connection_uri contains an invalid node: $node" }
        val host = runCatching { HttpHost.create(node) }
            .getOrElse { throw IllegalArgumentException("connection_uri contains an invalid node: $node", it) }
        host
    }.toTypedArray()
}
