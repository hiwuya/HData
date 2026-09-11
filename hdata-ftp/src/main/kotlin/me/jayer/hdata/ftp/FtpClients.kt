package me.jayer.hdata.ftp

import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPReply
import java.io.Serializable

/**
 * FTP connection parameters and client construction.
 *
 * @author wuya
 */
data class FtpConnection(
    val host: String = "",
    val hostName: String = "",
    val port: Int = 21,
    val user: String = "",
    val username: String = "",
    val password: String = "",
    /** Connection and read/write timeout (milliseconds). Without a timeout, an unresponsive peer would hang the job forever. */
    val timeoutMillis: Int = 30_000,
) : Serializable {

    val actualHost: String get() = host.ifBlank { hostName }

    val actualUser: String get() = user.ifBlank { username }

    fun validate() {
        require(actualHost.isNotBlank()) { "host must not be empty" }
        require(port in 1..65535) { "port must be in 1..65535" }
        require(timeoutMillis > 0) { "timeout_millis must be > 0" }
        require(host.isBlank() || hostName.isBlank() || host == hostName) {
            "host and host_name are both set with different values; keep only one"
        }
        require(user.isBlank() || username.isBlank() || user == username) {
            "user and username are both set with different values; keep only one"
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * Create a logged-in [FTPClient].
 *
 * Compared with the pre-refactor version, three things were added: connection/read-write timeouts
 * (previously an unresponsive peer would hang the job forever), the control connection encoding is
 * set to UTF-8 (otherwise Chinese file names would be garbled), and on failure the FTP reply text
 * is included in the exception.
 */
fun newFtpClient(connection: FtpConnection): FTPClient {
    val client = FTPClient()
    client.connectTimeout = connection.timeoutMillis
    client.controlEncoding = "UTF-8"
    try {
        client.connect(connection.actualHost, connection.port)
    } catch (e: Exception) {
        throw IllegalStateException("Failed to connect to FTP[${connection.actualHost}:${connection.port}]: ${e.message}", e)
    }
    if (!FTPReply.isPositiveCompletion(client.replyCode)) {
        val reply = client.replyString
        runCatching { client.disconnect() }
        throw IllegalStateException("Connection to FTP[${connection.actualHost}:${connection.port}] was refused: $reply")
    }
    client.setSoTimeout(connection.timeoutMillis)
    client.dataTimeout = java.time.Duration.ofMillis(connection.timeoutMillis.toLong())

    val user = connection.actualUser.ifBlank { "anonymous" }
    if (!client.login(user, connection.password)) {
        val reply = client.replyString
        runCatching { client.disconnect() }
        throw IllegalStateException("Failed to log in to FTP[${connection.actualHost}] (user $user): $reply")
    }
    client.setFileType(FTP.BINARY_FILE_TYPE)
    client.enterLocalPassiveMode()
    return client
}

/** A use-and-close client for one-off operations such as listing directories at graph-construction time. */
internal fun <T> withFtpClient(connection: FtpConnection, block: (FTPClient) -> T): T {
    val client = newFtpClient(connection)
    try {
        return block(client)
    } finally {
        runCatching { client.logout() }
        runCatching { client.disconnect() }
    }
}
