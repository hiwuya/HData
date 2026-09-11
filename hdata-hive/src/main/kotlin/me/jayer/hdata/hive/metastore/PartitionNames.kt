package me.jayer.hdata.hive.metastore

/**
 * Encode/decode of partition names and partition directory names, following Hive's `FileUtils.escapePathName` / `makePartName`.
 *
 * A Hive partition directory name is not a plain `col=value`: when the value contains `/`, `=`, `:` or `%`, or a control
 * character, it is `%XX` escaped, and null and the empty string are written as `__HIVE_DEFAULT_PARTITION__`. The pre-refactor
 * implementation spliced the output of `SHOW PARTITIONS` straight into an SQL predicate, without unescaping, and the `/` of
 *
 * @author wuya
 */
object PartitionNames {

    /** Placeholder written into the directory name when the value is null or empty, matching the default of
    const val DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"

    /** The set of characters that need escaping, character by character with Hive's `FileUtils.charToEscape`. */
    private val CHAR_TO_ESCAPE: BooleanArray = BooleanArray(128).apply {
        // All control characters
        for (c in 0 until 0x20) {
            this[c] = true
        }
        this[0x7F] = true
        """"#%'*/:=?\{[]^""".forEach { this[it.code] = true }
    }

    private fun needsEscape(c: Char): Boolean = c.code < 128 && CHAR_TO_ESCAPE[c.code]

    /** `2024-01-01` -> `2024-01-01`; `a/b` -> `a%2Fb`. */
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
     * `a%2Fb` -> `a/b`.
     *
     * A `%` not followed by two hex digits is taken literally — Hive itself is that forgiving, because directory names written
     * by some external tools really do contain a bare `%`.
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

    /** `["dt", "hr"] + ["2024-01-01", "01"]` -> `dt=2024-01-01/hr=01`. */
    fun makePartName(columnNames: List<String>, values: List<String?>): String {
        require(columnNames.size == values.size) {
            "partition column count and partition value count disagree: columns=$columnNames values=$values"
        }
        return columnNames.indices.joinToString("/") { i ->
            "${escapePathName(columnNames[i].lowercase())}=${escapePathName(values[i])}"
        }
    }

    /**
     * `dt=2024-01-01/hr=01` -> `["2024-01-01", "01"]`.
     *
     * Values are taken in the order of the partition name itself, without validating column names — the metastore guarantees
     * that the column order of a partition name matches [HiveTable.partitionColumns], while a directory may spell them differently.
     */
    fun toPartitionValues(partitionName: String): List<String> = partitionName
        .split('/')
        .filter { it.isNotBlank() }
        .map { part ->
            val idx = part.indexOf('=')
            require(idx > 0) { "cannot parse the partition name fragment: $part (full partition name: $partitionName)" }
            unescapePathName(part.substring(idx + 1))
        }

    /** Column names appearing in a partition name, used to check whether a partition name in the config matches the table. */
    fun toPartitionColumnNames(partitionName: String): List<String> = partitionName
        .split('/')
        .filter { it.isNotBlank() }
        .map { part ->
            val idx = part.indexOf('=')
            require(idx > 0) { "cannot parse the partition name fragment: $part (full partition name: $partitionName)" }
            unescapePathName(part.substring(0, idx))
        }

    /** Whether a partition value is the null placeholder. */
    fun isDefaultPartition(value: String): Boolean = value == DEFAULT_PARTITION
}
