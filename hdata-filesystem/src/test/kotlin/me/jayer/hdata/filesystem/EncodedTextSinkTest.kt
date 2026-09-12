package me.jayer.hdata.filesystem

import me.jayer.hdata.filesystem.transform.EncodedTextSink
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.nio.charset.Charset
import kotlin.test.Test
import kotlin.test.assertEquals

class EncodedTextSinkTest {

    @Test
    fun `writes header and content with the configured encoding without closing the FileIO channel`() {
        val bytes = ByteArrayOutputStream()
        val channel = Channels.newChannel(bytes)
        val sink = EncodedTextSink("GB18030", "name")

        sink.open(channel)
        sink.write("Alice")
        sink.flush()

        assertEquals("name${System.lineSeparator()}Alice${System.lineSeparator()}", bytes.toString(Charset.forName("GB18030")))
        // after flush the channel still belongs to FileIO, the sink must not close it on its own
        channel.write(java.nio.ByteBuffer.wrap("x".toByteArray()))
    }
}
