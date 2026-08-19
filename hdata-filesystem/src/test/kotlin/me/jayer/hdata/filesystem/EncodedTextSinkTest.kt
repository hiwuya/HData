package me.jayer.hdata.filesystem

import me.jayer.hdata.filesystem.transform.EncodedTextSink
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.nio.charset.Charset
import kotlin.test.Test
import kotlin.test.assertEquals

class EncodedTextSinkTest {

    @Test
    fun `按配置编码写表头与内容且不关闭 FileIO 通道`() {
        val bytes = ByteArrayOutputStream()
        val channel = Channels.newChannel(bytes)
        val sink = EncodedTextSink("GB18030", "姓名")

        sink.open(channel)
        sink.write("张三")
        sink.flush()

        assertEquals("姓名${System.lineSeparator()}张三${System.lineSeparator()}", bytes.toString(Charset.forName("GB18030")))
        // flush 后通道仍归 FileIO 所有，sink 不能擅自关闭
        channel.write(java.nio.ByteBuffer.wrap("x".toByteArray()))
    }
}
