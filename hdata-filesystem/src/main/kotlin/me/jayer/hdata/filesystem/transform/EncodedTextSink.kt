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
        checkNotNull(writer) { "text file is not open yet" }.flush()
        writer = null
    }

    private fun writeLine(value: String) {
        val out = checkNotNull(writer) { "text file is not open yet" }
        out.write(value)
        // TextIO's historical behavior and most data file conventions use LF; do not let it turn into CRLF depending on the worker OS.
        out.write('\n'.code)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
