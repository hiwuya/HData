package me.jayer.hdata.hive.metastore

/**
 * 分区名与分区目录名的编解码，规则与 Hive 的 `FileUtils.escapePathName` / `makePartName` 一致。
 *
 * Hive 的分区目录名不是简单的 `col=value`：值里出现 `/`、`=`、`:`、`%` 或控制字符时会做 `%XX` 转义，
 * null 与空串写成 `__HIVE_DEFAULT_PARTITION__`。重构前的实现直接把 `SHOW PARTITIONS` 的输出
 * 拼进 SQL 谓词，既没有反转义，多级分区的 `/` 还会把 SQL 拼坏。
 *
 * @author wuya
 */
object PartitionNames {

    /** 值为 null 或空串时写进目录名的占位符，与 `hive.exec.default.partition.name` 的默认值一致。 */
    const val DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"

    /** 需要转义的字符集合，逐字对齐 Hive `FileUtils.charToEscape`。 */
    private val CHAR_TO_ESCAPE: BooleanArray = BooleanArray(128).apply {
        // 所有控制字符
        for (c in 0 until 0x20) {
            this[c] = true
        }
        this[0x7F] = true
        """"#%'*/:=?\{[]^""".forEach { this[it.code] = true }
    }

    private fun needsEscape(c: Char): Boolean = c.code < 128 && CHAR_TO_ESCAPE[c.code]

    /** `2024-01-01` -> `2024-01-01`；`a/b` -> `a%2Fb`。 */
    fun escapePathName(value: String?): String {
        if (value.isNullOrEmpty()) {
            return DEFAULT_PARTITION
        }
        if (value.none(::needsEscape)) {
            return value
        }
        return buildString(value.length + 8) {
            value.forEach { c ->
                if (needsEscape(c)) {
                    append('%').append("%02X".format(c.code))
                } else {
                    append(c)
                }
            }
        }
    }

    /**
     * `a%2Fb` -> `a/b`。
     *
     * `%` 后面跟的不是两位十六进制时按字面量处理——Hive 自己也是这么容错的，
     * 有些外部工具写出来的目录名里就是带裸 `%` 的。
     */
    fun unescapePathName(value: String): String {
        if (!value.contains('%')) {
            return value
        }
        val sb = StringBuilder(value.length)
        var i = 0
        while (i < value.length) {
            val c = value[i]
            if (c == '%' && i + 2 < value.length) {
                val code = value.substring(i + 1, i + 3).toIntOrNull(16)
                if (code != null) {
                    sb.append(code.toChar())
                    i += 3
                    continue
                }
            }
            sb.append(c)
            i++
        }
        return sb.toString()
    }

    /** `["dt", "hr"] + ["2024-01-01", "01"]` -> `dt=2024-01-01/hr=01`。 */
    fun makePartName(columnNames: List<String>, values: List<String?>): String {
        require(columnNames.size == values.size) {
            "分区列与分区值个数不一致: 列=$columnNames 值=$values"
        }
        return columnNames.indices.joinToString("/") { i ->
            "${escapePathName(columnNames[i].lowercase())}=${escapePathName(values[i])}"
        }
    }

    /**
     * `dt=2024-01-01/hr=01` -> `["2024-01-01", "01"]`。
     *
     * 只按分区名本身的顺序取值，不校验列名——metastore 保证分区名的列顺序与
     * [HiveTable.partitionColumns] 一致，而目录里可能出现大小写不同的写法。
     */
    fun toPartitionValues(partitionName: String): List<String> = partitionName
        .split('/')
        .filter { it.isNotBlank() }
        .map { part ->
            val idx = part.indexOf('=')
            require(idx > 0) { "无法解析分区名片段: $part（完整分区名: $partitionName）" }
            unescapePathName(part.substring(idx + 1))
        }

    /** 分区名里出现的列名，用于校验配置里写的分区名是否对得上表。 */
    fun toPartitionColumnNames(partitionName: String): List<String> = partitionName
        .split('/')
        .filter { it.isNotBlank() }
        .map { part ->
            val idx = part.indexOf('=')
            require(idx > 0) { "无法解析分区名片段: $part（完整分区名: $partitionName）" }
            unescapePathName(part.substring(0, idx))
        }

    /** 分区值是否是 null 占位符。 */
    fun isDefaultPartition(value: String): Boolean = value == DEFAULT_PARTITION
}
