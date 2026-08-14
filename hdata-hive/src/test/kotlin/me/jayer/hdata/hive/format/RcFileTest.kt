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
 * RCFile 容器格式与 LazyBinary 单值编码。
 *
 * 这两块是本模块里唯一**自己按格式规范实现**的部分（没有引 hive-exec），
 * 所以要单独把边界情况钉住：游程编码的单元格长度、同步标记的定位、变长整数。
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
    fun `写出去再读回来，行内容一致`() {
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
    fun `定长列走游程编码也能正确切开`() {
        // 长度相同的连续单元格会被压成"长度 + 重复次数"，解码时必须按同样的规则展开
        val rows = (1..50).map { cells("%03d".format(it), "xx") }
        val read = writeAndRead(rows, 2)
        assertEquals(50, read.size)
        assertEquals("001", read.first()[0])
        assertEquals("050", read.last()[0])
        assertTrue(read.all { it[1] == "xx" })
    }

    @Test
    fun `行数很多时会插入同步标记，仍然读得完整`() {
        // 每 2000 字节插一个同步块，5000 行必然跨过很多个
        val rows = (1..5000).map { cells(it.toString(), "value-$it") }
        val read = writeAndRead(rows, 2)
        assertEquals(5000, read.size)
        assertEquals(listOf("5000", "value-5000"), read.last())
    }

    /**
     * 读一个**真正由 Hive 写出来**的 RCFile。
     *
     * `src/test/resources/hive-written.rc` 是用 hive-exec 4.0.1 的 `RCFile.Writer` 生成的
     * （2000 行 3 列，第 3 列每 3 行一个空值），提前生成好放进仓库，这样测试不需要依赖 hive-exec。
     * 反过来的方向也验证过：Hive 的 `RCFile.Reader` 能完整读出本实现写的文件，
     * 两边写出来的文件除了那 16 字节随机同步标记之外**逐字节相同**。
     */
    @Test
    fun `能读 Hive 自己写出来的 RCFile`() {
        val resource = checkNotNull(javaClass.getResourceAsStream("/hive-written.rc")) {
            "缺少测试夹具 hive-written.rc"
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
            // Hive 侧第 3 列每 3 行写一个空串，读出来是长度 0 的单元格
            assertEquals(listOf("3", "name-3", null), rows[2])
            assertEquals(666, rows.count { it[2] == null })
        } finally {
            Files.deleteIfExists(file)
        }
    }

    @Test
    fun `不是 RCFile 时明确报错`() {
        val file = Files.createTempFile("notrc-", ".rc")
        try {
            Files.write(file, "hello world, definitely not an RCFile".toByteArray())
            val fs = FileSystem.get(file.toUri(), configuration)
            val error = assertFailsWith<IllegalArgumentException> {
                RcFileReader(fs.open(Path(file.toUri())), Files.size(file), configuration)
            }
            assertTrue(error.message!!.contains("不是 RCFile"))
        } finally {
            Files.deleteIfExists(file)
        }
    }

    @Test
    fun `变长整数的解码与 Hadoop 一致`() {
        listOf(0L, 1L, -1L, 127L, 128L, -128L, 300L, -300L, Long.MAX_VALUE, Long.MIN_VALUE).forEach { value ->
            val bytes = java.io.ByteArrayOutputStream()
            java.io.DataOutputStream(bytes).use { org.apache.hadoop.io.WritableUtils.writeVLong(it, value) }
            val decoded = RcFile.readVLong(bytes.toByteArray(), 0)
            assertEquals(value, decoded.value, "变长整数 $value 解码不对")
            assertEquals(bytes.size(), decoded.length)
        }
    }

    @Test
    fun `LazyBinary 单值编解码往返`() {
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
            assertEquals(value, LazyBinaryCodec.decode(encoded, encoded.indices, fieldType), "$fieldType 往返不一致")
        }
    }

    @Test
    fun `LazyBinary 的 timestamp 编码保留纳秒`() {
        // 纳秒是十进制反转后存的，尾零最容易出错
        listOf(
            Instant.ofEpochSecond(0),
            Instant.ofEpochSecond(1_700_000_000L),
            Instant.ofEpochSecond(1_700_000_000L, 123_000_000L),
            Instant.ofEpochSecond(1_700_000_000L, 999_999_999L),
            // 超过 int 秒范围，要走第二个变长整数
            Instant.ofEpochSecond(4_000_000_000L, 1L),
        ).forEach { instant ->
            val encoded = LazyBinaryCodec.encode(instant, FieldTypes.TIMESTAMP)
            assertEquals(instant, LazyBinaryCodec.decode(encoded, encoded.indices, FieldTypes.TIMESTAMP))
        }
    }

    @Test
    fun `长度为 0 的单元格读成 null`() {
        assertNull(LazyBinaryCodec.decode(ByteArray(0), IntRange.EMPTY, FieldTypes.STRING))
        assertNull(LazyBinaryCodec.decode(ByteArray(0), IntRange.EMPTY, FieldTypes.INT32))
    }

    @Test
    fun `RCBINARY 不支持嵌套类型时明确报错`() {
        val arrayType = org.apache.beam.sdk.schemas.Schema.FieldType.array(FieldTypes.STRING)
        val error = assertFailsWith<UnsupportedOperationException> {
            LazyBinaryCodec.encode(listOf("a"), arrayType)
        }
        assertTrue(error.message!!.contains("RCBINARY"))
    }
}
