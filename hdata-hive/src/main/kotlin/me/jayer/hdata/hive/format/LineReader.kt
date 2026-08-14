package me.jayer.hdata.hive.format

import java.io.InputStream

/**
 * 按行读字节流，同时记录**当前行的起始偏移量**。
 *
 * 偏移量是按字节区间并行读的关键：SDF 认领的就是行的起始位置。
 * `BufferedReader.readLine()` 拿不到这个数，所以只能自己数字节。
 *
 * 行尾同时认 `\n` 与 `\r\n`（以及单独的 `\r`），与 Hadoop 的 `LineRecordReader` 一致。
 * 自带缓冲，`\r` 后面要不要吃掉 `\n` 直接看缓冲区里的下一个字节，不需要 pushback。
 *
 * @author wuya
 */
class LineReader(private val input: InputStream, startPosition: Long, bufferSize: Int = 64 * 1024) {

    /** 下一行的起始偏移量。 */
    var position: Long = startPosition
        private set

    private val buffer = ByteArray(bufferSize)
    private var bufferLength = 0
    private var bufferOffset = 0

    private var line = ByteArray(256)
    private var lineLength = 0

    /** @return 一行的字节（不含行尾），到文件末尾返回 null。 */
    fun readLine(): ByteArray? {
        lineLength = 0
        var consumed = 0L
        while (true) {
            val b = read()
            if (b < 0) {
                if (consumed == 0L) {
                    return null
                }
                position += consumed
                return line.copyOf(lineLength)
            }
            consumed++
            when (b) {
                LF -> {
                    position += consumed
                    return line.copyOf(lineLength)
                }

                CR -> {
                    if (peek() == LF) {
                        read()
                        consumed++
                    }
                    position += consumed
                    return line.copyOf(lineLength)
                }

                else -> append(b.toByte())
            }
        }
    }

    private fun append(b: Byte) {
        if (lineLength == line.size) {
            line = line.copyOf(line.size * 2)
        }
        line[lineLength++] = b
    }

    private fun read(): Int {
        if (!fill()) {
            return -1
        }
        return buffer[bufferOffset++].toInt() and 0xFF
    }

    private fun peek(): Int {
        if (!fill()) {
            return -1
        }
        return buffer[bufferOffset].toInt() and 0xFF
    }

    private fun fill(): Boolean {
        if (bufferOffset < bufferLength) {
            return true
        }
        bufferLength = input.read(buffer)
        bufferOffset = 0
        return bufferLength > 0
    }

    private companion object {
        const val LF = '\n'.code
        const val CR = '\r'.code
    }
}
