package me.jayer.hdata.ftp

import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPReply
import java.io.Serializable

/**
 * FTP 连接参数与客户端构造。
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
    /** 连接与读写超时（毫秒）。没有超时的话，对端假死会让作业一直挂着。 */
    val timeoutMillis: Int = 30_000,
) : Serializable {

    val actualHost: String get() = host.ifBlank { hostName }

    val actualUser: String get() = user.ifBlank { username }

    fun validate() {
        require(actualHost.isNotBlank()) { "host 不能为空" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 创建一个已登录的 [FTPClient]。
 *
 * 相比重构前多做了三件事：设置连接/读写超时（此前对端假死会让作业永远挂着）、
 * 把控制连接的编码设成 UTF-8（否则中文文件名会乱码）、失败时把 FTP 的应答文本一并带进异常。
 */
fun newFtpClient(connection: FtpConnection): FTPClient {
    val client = FTPClient()
    client.connectTimeout = connection.timeoutMillis
    client.controlEncoding = "UTF-8"
    try {
        client.connect(connection.actualHost, connection.port)
    } catch (e: Exception) {
        throw IllegalStateException("连接 FTP[${connection.actualHost}:${connection.port}] 失败: ${e.message}", e)
    }
    if (!FTPReply.isPositiveCompletion(client.replyCode)) {
        val reply = client.replyString
        runCatching { client.disconnect() }
        throw IllegalStateException("连接 FTP[${connection.actualHost}:${connection.port}] 被拒绝: $reply")
    }
    client.setSoTimeout(connection.timeoutMillis)
    client.dataTimeout = java.time.Duration.ofMillis(connection.timeoutMillis.toLong())

    val user = connection.actualUser.ifBlank { "anonymous" }
    if (!client.login(user, connection.password)) {
        val reply = client.replyString
        runCatching { client.disconnect() }
        throw IllegalStateException("登录 FTP[${connection.actualHost}] 失败（用户 $user）: $reply")
    }
    client.setFileType(FTP.BINARY_FILE_TYPE)
    client.enterLocalPassiveMode()
    return client
}

/** 用完即关的客户端，用于构图阶段列目录这类一次性操作。 */
internal fun <T> withFtpClient(connection: FtpConnection, block: (FTPClient) -> T): T {
    val client = newFtpClient(connection)
    try {
        return block(client)
    } finally {
        runCatching { client.logout() }
        runCatching { client.disconnect() }
    }
}
