package me.jayer.hdata.hive.format

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.rcfile.LazyBinaryCodec
import me.jayer.hdata.hive.format.rcfile.RcFile
import me.jayer.hdata.hive.format.rcfile.RcFileReader
import me.jayer.hdata.hive.format.rcfile.RcFileWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.math.BigDecimal
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The RCFile container format and the LazyBinary single-value encoding.
 *
 * These two are the only parts of this module **implemented from scratch against the format spec** (without hive-exec),
 * so the edge cases are pinned down separately: run-length encoded cell lengths, sync marker location, variable-length integers.
 *
 * @author wuya
 */
class RcFileTest {

    private val configuration = Configuration()

    private fun writeAndRead(rows: List<Array<ByteArray?>>, columnCount: Int): List<List<String?>> {
        val file = Files.createTempFile("rcfile-", ".rc")
        try {
            Files.newOutputStream(file).use { out ->
                RcFileWriter(out, columnCount).use { writer -> rows.forEach(writer::append) }
            }
            val path = Path(file.toUri())
            val fs = FileSystem.get(file.toUri(), configuration)
            return RcFileReader(fs.open(path), Files.size(file), configuration).use { reader ->
                assertEquals(columnCount, reader.columnCount)
                buildList {
                    while (true) {
                        val block = reader.nextBlock() ?: break
                        repeat(block.rowCount) {
                            add(
                                block.columns.map { column ->
                                    if (column.allNull) {
                                        null
                                    } else {
                                        val cell = column.nextCell()
                                        if (cell.isEmpty()) {
                                            null
                                        } else {
                                            String(column.data, cell.first, cell.last - cell.first + 1)
                                        }
                                    }
                                }
                            )
                        }
                    }
                }
            }
        } finally {
            Files.deleteIfExists(file)
        }
    }

    private fun cells(vararg values: String?): Array<ByteArray?> =
        values.map { it?.toByteArray() }.toTypedArray()

    @Test
    fun `writes out then reads back, row content matches`() {
        val rows = listOf(
            cells("1", "张三"),
            cells("22", "李四"),
            cells("333", null),
        )
        assertEquals(
            listOf(listOf("1", "张三"), listOf("22", "李四"), listOf("333", null)),
            writeAndRead(rows, 2),
        )
    }

    @Test
    fun `run-length-encoded fixed-length columns still split correctly`() {
        // Consecutive cells of the same length are compressed into "length + repeat count" and must be expanded by the same rule
        val rows = (1..50).map { cells("%03d".format(it), "xx") }
        val read = writeAndRead(rows, 2)
        assertEquals(50, read.size)
        assertEquals("001", read.first()[0])
        assertEquals("050", read.last()[0])
        assertTrue(read.all { it[1] == "xx" })
    }

    @Test
    fun `sync markers get inserted with many rows, and the file still reads back complete`() {
        // A sync block is inserted every 2000 bytes, so 5000 rows necessarily cross many of them
        val rows = (1..5000).map { cells(it.toString(), "value-$it") }
        val read = writeAndRead(rows, 2)
        assertEquals(5000, read.size)
        assertEquals(listOf("5000", "value-5000"), read.last())
    }

    /**
     * Reads an RCFile **really written by Hive**.
     *
     * `src/test/resources/hive-written.rc` was generated with `RCFile.Writer` from hive-exec 4.0.1
     * (2000 rows, 3 columns, an empty value in every third row of the third column), pre-generated and checked into the repo so
     * the test does not need to depend on hive-exec. The reverse direction is verified too: Hive's `RCFile.Reader` can fully
     * read the file written by this implementation, and the two files are **byte-for-byte identical** except for that
     * 16-byte random sync marker.
     */
    @Test
    fun `reads an RCFile actually written by Hive`() {
        val resource = checkNotNull(javaClass.getResourceAsStream("/hive-written.rc")) {
            "missing the test fixture hive-written.rc"
        }
        val file = Files.createTempFile("hive-written-", ".rc")
        try {
            resource.use { input -> Files.newOutputStream(file).use { input.copyTo(it) } }
            val fs = FileSystem.get(file.toUri(), configuration)
            val rows = RcFileReader(fs.open(Path(file.toUri())), Files.size(file), configuration).use { reader ->
                assertEquals(3, reader.columnCount)
                buildList {
                    while (true) {
                        val block = reader.nextBlock() ?: break
                        repeat(block.rowCount) {
                            add(
                                block.columns.map { column ->
                                    val cell = column.nextCell()
                                    if (cell.isEmpty()) null else String(column.data, cell.first, cell.last - cell.first + 1)
                                }
                            )
                        }
                    }
                }
            }
            assertEquals(2000, rows.size)
            assertEquals(listOf("1", "name-1", "v1"), rows.first())
            assertEquals(listOf("2000", "name-2000", "v2000"), rows.last())
            // Hive writes an empty string into every third row of the third column, which reads back as a zero-length cell
            assertEquals(listOf("3", "name-3", null), rows[2])
            assertEquals(666, rows.count { it[2] == null })
        } finally {
            Files.deleteIfExists(file)
        }
    }

    @Test
    fun `a non-RCFile input errors clearly`() {
        val file = Files.createTempFile("notrc-", ".rc")
        try {
            Files.write(file, "hello world, definitely not an RCFile".toByteArray())
            val fs = FileSystem.get(file.toUri(), configuration)
            val error = assertFailsWith<IllegalArgumentException> {
                RcFileReader(fs.open(Path(file.toUri())), Files.size(file), configuration)
            }
            assertTrue(error.message!!.contains("not an RCFile"))
        } finally {
            Files.deleteIfExists(file)
        }
    }

    @Test
    fun `variable-length integer decoding matches Hadoop's`() {
        listOf(0L, 1L, -1L, 127L, 128L, -128L, 300L, -300L, Long.MAX_VALUE, Long.MIN_VALUE).forEach { value ->
            val bytes = java.io.ByteArrayOutputStream()
            java.io.DataOutputStream(bytes).use { org.apache.hadoop.io.WritableUtils.writeVLong(it, value) }
            val decoded = RcFile.readVLong(bytes.toByteArray(), 0)
            assertEquals(value, decoded.value, "variable-length integer $value decoded incorrectly")
            assertEquals(bytes.size(), decoded.length)
        }
    }

    @Test
    fun `LazyBinary single-value codec round-trips`() {
        val cases = listOf<Pair<Any, org.apache.beam.sdk.schemas.Schema.FieldType>>(
            true to FieldTypes.BOOLEAN,
            7.toByte() to FieldTypes.BYTE,
            (-300).toShort() to FieldTypes.INT16,
            123456 to FieldTypes.INT32,
            -9_000_000_000L to FieldTypes.INT64,
            1.5f to FieldTypes.FLOAT,
            2.25 to FieldTypes.DOUBLE,
            "中文" to FieldTypes.STRING,
            BigDecimal("123.450") to FieldTypes.DECIMAL,
            LocalDate.of(2024, 2, 29) to FieldTypes.DATE,
        )
        cases.forEach { (value, fieldType) ->
            val encoded = LazyBinaryCodec.encode(value, fieldType)
            assertEquals(value, LazyBinaryCodec.decode(encoded, encoded.indices, fieldType), "$fieldType round trip does not match")
        }
    }

    @Test
    fun `LazyBinary's timestamp encoding preserves nanoseconds`() {
        // Nanoseconds are stored decimal-reversed, so trailing zeros are the easiest thing to get wrong
        listOf(
            Instant.ofEpochSecond(0),
            Instant.ofEpochSecond(1_700_000_000L),
            Instant.ofEpochSecond(1_700_000_000L, 123_000_000L),
            Instant.ofEpochSecond(1_700_000_000L, 999_999_999L),
            // Past the range of int seconds, so a second variable-length integer is needed
            Instant.ofEpochSecond(4_000_000_000L, 1L),
        ).forEach { instant ->
            val encoded = LazyBinaryCodec.encode(instant, FieldTypes.TIMESTAMP)
            assertEquals(instant, LazyBinaryCodec.decode(encoded, encoded.indices, FieldTypes.TIMESTAMP))
        }
    }

    @Test
    fun `a zero-length cell reads back as null`() {
        assertNull(LazyBinaryCodec.decode(ByteArray(0), IntRange.EMPTY, FieldTypes.STRING))
        assertNull(LazyBinaryCodec.decode(ByteArray(0), IntRange.EMPTY, FieldTypes.INT32))
    }

    @Test
    fun `RCBINARY errors clearly on an unsupported nested type`() {
        val arrayType = org.apache.beam.sdk.schemas.Schema.FieldType.array(FieldTypes.STRING)
        val error = assertFailsWith<UnsupportedOperationException> {
            LazyBinaryCodec.encode(listOf("a"), arrayType)
        }
        assertTrue(error.message!!.contains("RCBINARY"))
    }
}
