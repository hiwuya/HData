package me.jayer.hdata.filesystem.transform

import org.apache.beam.sdk.io.FileIO
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.Charset
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel

/** FileIO owns the channel; this sink flushes but never closes it. */
class EncodedTextSink(
    private val encoding: String,
    private val header: String?,
) : FileIO.Sink<String> {

    @Transient
    private var writer: BufferedWriter? = null

    override fun open(channel: WritableByteChannel) {
        writer = BufferedWriter(OutputStreamWriter(Channels.newOutputStream(channel), Charset.forName(encoding)))
        header?.let { writeLine(it) }
    }

    override fun write(element: String) = writeLine(element)

    override fun flush() {
        checkNotNull(writer) { "文本文件尚未打开" }.flush()
        writer = null
    }

    private fun writeLine(value: String) {
        val out = checkNotNull(writer) { "文本文件尚未打开" }
        out.write(value)
        // TextIO 的历史行为与大多数数据文件约定都是 LF；不要随 worker 操作系统变成 CRLF。
        out.write('\n'.code)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
