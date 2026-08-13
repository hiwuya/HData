package me.jayer.hdata.ftp

import org.apache.ftpserver.FtpServer
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.usermanager.impl.WritePermission
import java.io.File
import java.net.ServerSocket
import java.nio.file.Files

/**
 * 进程内 FTP 服务器，端到端测试用，不需要任何外部服务。
 *
 * 用 Apache FtpServer 起一个真的 FTP 服务，测试因此覆盖到真实的 `REST` / `STOR` / `APPE` / `RNFR-RNTO`
 * 行为——这些恰恰是重构里改动最大的地方，用 mock 是测不出来的。
 *
 * @author wuya
 */
class EmbeddedFtpServer : AutoCloseable {

    val home: File = Files.createTempDirectory("hdata-ftp-home").toFile()
    val port: Int = freePort()
    val user: String = "hdata"
    val password: String = "hdata"

    private val server: FtpServer

    init {
        val userManagerFile = File(home.parentFile, "users-${System.nanoTime()}.properties")
        userManagerFile.writeText("")

        val factory = FtpServerFactory()
        val listener = ListenerFactory().apply { this.port = this@EmbeddedFtpServer.port }
        factory.addListener("default", listener.createListener())
        // DirectRunner 会起很多 bundle，每个 DoFn 实例一条连接；默认上限 10 会让后来的直接被 421 顶掉
        factory.connectionConfig = org.apache.ftpserver.ConnectionConfigFactory().apply {
            maxLogins = 200
            maxAnonymousLogins = 0
            maxThreads = 200
        }.createConnectionConfig()

        val account = BaseUser().apply {
            name = user
            password = this@EmbeddedFtpServer.password
            homeDirectory = home.absolutePath
            authorities = listOf(WritePermission())
        }
        factory.userManager.save(account)

        server = factory.createServer()
        server.start()
    }

    /** 在服务器根目录下放一个文件。 */
    fun put(relativePath: String, content: String): File {
        val file = File(home, relativePath)
        file.parentFile.mkdirs()
        file.writeText(content, Charsets.UTF_8)
        return file
    }

    /** 读回服务器根目录下的某个文件。 */
    fun read(relativePath: String): String = File(home, relativePath).readText(Charsets.UTF_8)

    /** 列出某个目录下的文件名。 */
    fun list(relativePath: String): List<String> =
        (File(home, relativePath).listFiles() ?: emptyArray()).filter { it.isFile }.map { it.name }.sorted()

    /** 拼出连接配置；[extra] 按行追加，避免多行字符串插值把缩进弄乱。 */
    fun readConfigYaml(path: String, extra: String = ""): String = buildString {
        appendLine("""host: "127.0.0.1"""")
        appendLine("port: $port")
        appendLine("""user: "$user"""")
        appendLine("""password: "$password"""")
        appendLine("""path: "$path"""")
        extra.lines().filter { it.isNotBlank() }.forEach { appendLine(it.trimIndent()) }
    }

    override fun close() {
        runCatching { server.stop() }
        runCatching { home.deleteRecursively() }
    }

    private fun freePort(): Int = ServerSocket(0).use { it.localPort }
}
