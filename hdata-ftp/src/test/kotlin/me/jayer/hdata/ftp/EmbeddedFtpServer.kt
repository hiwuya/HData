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
 * An in-process FTP server for end-to-end tests; requires no external service.
 *
 * It starts a real FTP service using Apache FtpServer, so the tests cover the real `REST` / `STOR` /
 * `APPE` / `RNFR-RNTO` behavior — exactly the parts most changed in the refactor, which a mock could
 * never exercise.
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
        // DirectRunner spins up many bundles, one connection per DoFn instance; the default limit of
        // 10 would let later ones get kicked with 421
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

    /** Place a file under the server's root directory. */
    fun put(relativePath: String, content: String): File {
        val file = File(home, relativePath)
        file.parentFile.mkdirs()
        file.writeText(content, Charsets.UTF_8)
        return file
    }

    /** Read back a file under the server's root directory. */
    fun read(relativePath: String): String = File(home, relativePath).readText(Charsets.UTF_8)

    /** List the file names under a directory. */
    fun list(relativePath: String): List<String> =
        (File(home, relativePath).listFiles() ?: emptyArray()).filter { it.isFile }.map { it.name }.sorted()

    /** Build the connection config; [extra] is appended line by line to avoid the indentation being mangled by multi-line string interpolation. */
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
