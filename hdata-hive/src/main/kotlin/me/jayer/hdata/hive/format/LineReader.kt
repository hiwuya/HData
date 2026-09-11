package me.jayer.hdata.hive.format

import java.io.InputStream

/**
 * Reads a byte stream line by line, keeping track of the **start offset of the current line**.
 *
 * That offset is the key to parallel reads by byte range: it is the line start position the SDF claims. `BufferedReader.readLine()`
 * cannot give that number, so we count the bytes ourselves.
 *
 * Both `\n` and `\r\n` (and a lone `\r`) are accepted as line endings, consistent with Hadoop's `LineRecordReader`. It has its
 * own buffer, so whether to swallow the `\n` after a `\r` is decided by looking at the next byte in the buffer; no pushback needed.
 *
 * @author wuya
 */
class LineReader(private val input: InputStream, startPosition: Long, bufferSize: Int = 64 * 1024) {

    /** Start offset of the next line. */
    var position: Long = startPosition
        private set

    private val buffer = ByteArray(bufferSize)
    private var bufferLength = 0
    private var bufferOffset = 0

    private var line = ByteArray(256)
    private var lineLength = 0

    /** @return the bytes of one line (without the line ending), or null at end of file. */
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
