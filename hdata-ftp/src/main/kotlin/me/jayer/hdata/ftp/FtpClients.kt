package me.jayer.hdata.ftp

import org.apache.commons.net.ftp.FTPClient
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * FTP 连接参数。配置键对齐 Flink FTP filesystem connector：`host` / `host_name`、
 * `port`(默认 21)、`user` / `username`、`password`、`path`。
 */
data class FtpConnection(
    val host: String = "",
    val hostName: String = "",
    val port: Int = 21,
    val user: String = "",
    val username: String = "",
    val password: String = "",
) : Serializable {

    val actualHost: String get() = if (host.isNotBlank()) host else hostName

    val actualUser: String get() = if (user.isNotBlank()) user else username

    fun validate() {
        require(actualHost.isNotBlank()) { "host 不能为空" }
    }
}

/** 创建一个已登录的 [FTPClient]。 */
fun newFtpClient(connection: FtpConnection): FTPClient {
    val client = FTPClient()
    client.connect(connection.actualHost, connection.port)
    val reply = client.replyCode
    if (!org.apache.commons.net.ftp.FTPReply.isPositiveCompletion(reply)) {
        client.disconnect()
        throw IllegalStateException("连接 FTP[${connection.actualHost}:${connection.port}] 失败，reply=$reply")
    }
    val user = if (connection.actualUser.isNotBlank()) connection.actualUser else "anonymous"
    val pwd = connection.password.ifBlank { "" }
    if (!client.login(user, pwd)) {
        client.logout()
        client.disconnect()
        throw IllegalStateException("登录 FTP[${connection.actualHost}] 失败")
    }
    client.setFileType(org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE)
    client.enterLocalPassiveMode()
    return client
}

private val LOGGER = LoggerFactory.getLogger("me.jayer.hdata.ftp.FtpClients")
