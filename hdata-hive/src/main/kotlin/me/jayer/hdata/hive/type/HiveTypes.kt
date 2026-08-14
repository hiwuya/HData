package me.jayer.hdata.hive.type

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveTable
import org.apache.beam.sdk.schemas.Schema

/**
 * Hive 类型字符串 <-> Beam 类型。
 *
 * metastore 里列的类型是一个字符串，嵌套类型直接写在里面：
 * `array<struct<id:int,tags:map<string,decimal(10,2)>>>`。所以不能按逗号 split，
 * 必须真的解析一遍尖括号——这也是重构前那版 Hive 连接器推不出类型就**静默退化成单列
 * `value STRING`** 的根因。
 *
 * 时间类型的映射按 Hive 的语义来，不是照着名字猜：
 *  - `timestamp` 在 Hive 里是**不带时区的墙上时间**，映射到 [FieldTypes.DATETIME]（`LocalDateTime`）；
 *  - `timestamp with local time zone` 才是时间点，映射到 [FieldTypes.TIMESTAMP]（`Instant`）。
 * 反过来映射（DATETIME -> `timestamp`）也照这个走，跨两端同步才不会平白差一个时区。
 *
 * @author wuya
 */
object HiveTypes {

    /** 把一张表的列变成 Beam schema。分区列排在数据列之后，与 Hive `SELECT *` 的顺序一致。 */
    fun schemaOf(table: HiveTable): Schema = schemaOf(table.columns)

    fun schemaOf(columns: List<HiveColumn>): Schema = Schema.builder().apply {
        columns.forEach { column ->
            // Hive 的列一律可空，没有 NOT NULL 约束
            addField(Schema.Field.nullable(column.name, parse(column.type)))
        }
    }.build()

    /**
     * 解析一个 Hive 类型字符串。
     *
     * @throws IllegalArgumentException 类型不认识或写法不合法。**不做兜底**：
     *   一列推不出类型就整个作业失败，好过读出一堆对不上的数据。
     */
    fun parse(type: String): Schema.FieldType {
        val parser = TypeParser(type)
        val result = parser.parseType()
        parser.expectEnd()
        return result
    }

    /**
     * Beam 类型 -> Hive 类型字符串，建表时用。
     *
     * `SqlTypes.TIME` 没有对应的 Hive 类型（Hive 至今没有 TIME），映射成 `string`，
     * 与其建出一张读不回来的表，不如明确地存成字符串。
     */
    fun toHiveType(fieldType: Schema.FieldType): String = when (fieldType.typeName) {
        Schema.TypeName.BYTE -> "tinyint"
        Schema.TypeName.INT16 -> "smallint"
        Schema.TypeName.INT32 -> "int"
        Schema.TypeName.INT64 -> "bigint"
        Schema.TypeName.FLOAT -> "float"
        Schema.TypeName.DOUBLE -> "double"
        Schema.TypeName.BOOLEAN -> "boolean"
        Schema.TypeName.STRING -> "string"
        Schema.TypeName.BYTES -> "binary"
        Schema.TypeName.DECIMAL -> "decimal(38,18)"
        Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE ->
            "array<${toHiveType(fieldType.collectionElementType!!)}>"

        Schema.TypeName.MAP ->
            "map<${toHiveType(fieldType.mapKeyType!!)},${toHiveType(fieldType.mapValueType!!)}>"

        Schema.TypeName.ROW -> fieldType.rowSchema!!.fields.joinToString(
            separator = ",",
            prefix = "struct<",
            postfix = ">",
        ) { "${it.name}:${toHiveType(it.type)}" }

        Schema.TypeName.LOGICAL_TYPE -> when (fieldType.withNullable(false)) {
            FieldTypes.DATE -> "date"
            FieldTypes.DATETIME -> "timestamp"
            FieldTypes.TIMESTAMP -> "timestamp with local time zone"
            FieldTypes.TIME -> "string"
            else -> throw IllegalArgumentException("暂不支持的逻辑类型: ${fieldType.logicalType?.identifier}")
        }

        else -> throw IllegalArgumentException("暂不支持的 Beam 类型: $fieldType")
    }

    /** decimal 的精度与标度，写 ORC / Parquet 时要按列声明。 */
    fun decimalPrecisionAndScale(type: String): Pair<Int, Int> {
        val normalized = type.trim().lowercase()
        require(normalized.startsWith("decimal") || normalized.startsWith("numeric")) {
            "不是 decimal 类型: $type"
        }
        val args = normalized.substringAfter('(', "").substringBefore(')')
        if (args.isBlank()) {
            // Hive 0.12 及以前 decimal 不带参数，等价于 decimal(10,0)
            return 10 to 0
        }
        val parts = args.split(',').map { it.trim().toInt() }
        return when (parts.size) {
            1 -> parts[0] to 0
            2 -> parts[0] to parts[1]
            else -> throw IllegalArgumentException("decimal 参数写法不合法: $type")
        }
    }

    /**
     * 一个手写的递归下降解析器。
     *
     * 用它而不是正则：`map<string,array<struct<a:int,b:string>>>` 里的逗号有三层含义，
     * 正则分不开。
     */
    private class TypeParser(private val input: String) {

        private var pos = 0

        fun parseType(): Schema.FieldType {
            skipSpaces()
            val name = readIdentifier().lowercase()
            return when (name) {
                "tinyint", "byte" -> FieldTypes.BYTE
                "smallint", "short" -> FieldTypes.INT16
                "int", "integer" -> FieldTypes.INT32
                "bigint", "long" -> FieldTypes.INT64
                "float", "real" -> FieldTypes.FLOAT
                "double" -> FieldTypes.DOUBLE
                "boolean" -> FieldTypes.BOOLEAN
                "binary" -> FieldTypes.BYTES
                "date" -> FieldTypes.DATE
                "string" -> FieldTypes.STRING
                // 长度参数对 Beam 侧没有意义，读掉丢弃
                "varchar", "char" -> FieldTypes.STRING.also { skipParenthesizedArgs() }
                "decimal", "numeric" -> FieldTypes.DECIMAL.also { skipParenthesizedArgs() }
                "timestamp" -> parseTimestamp()
                "array" -> Schema.FieldType.array(parseAngleBracketed { parseType() }.withNullable(true))
                "map" -> parseMap()
                "struct" -> parseStruct()
                "uniontype" -> throw IllegalArgumentException("Hive 的 uniontype 暂不支持: $input")
                "void" -> throw IllegalArgumentException("Hive 的 void 类型暂不支持: $input")
                else -> throw IllegalArgumentException("无法识别的 Hive 类型: $name（完整类型: $input）")
            }
        }

        /** `timestamp` / `timestamp with local time zone`，后者才是带时区的时间点。 */
        private fun parseTimestamp(): Schema.FieldType {
            val rest = input.substring(pos).trimStart().lowercase()
            if (rest.startsWith("with local time zone")) {
                pos = input.indexOf("zone", pos, ignoreCase = true) + "zone".length
                return FieldTypes.TIMESTAMP
            }
            return FieldTypes.DATETIME
        }

        private fun parseMap(): Schema.FieldType {
            expect('<')
            val keyType = parseType()
            skipSpaces()
            expect(',')
            val valueType = parseType()
            skipSpaces()
            expect('>')
            return Schema.FieldType.map(keyType, valueType.withNullable(true))
        }

        private fun parseStruct(): Schema.FieldType {
            expect('<')
            val builder = Schema.builder()
            while (true) {
                skipSpaces()
                val fieldName = readIdentifier()
                require(fieldName.isNotEmpty()) { "struct 字段名不能为空（完整类型: $input）" }
                skipSpaces()
                expect(':')
                builder.addField(Schema.Field.nullable(fieldName, parseType()))
                skipSpaces()
                if (peek() == ',') {
                    pos++
                    continue
                }
                expect('>')
                return Schema.FieldType.row(builder.build())
            }
        }

        private fun <T> parseAngleBracketed(block: () -> T): T {
            expect('<')
            val result = block()
            skipSpaces()
            expect('>')
            return result
        }

        private fun readIdentifier(): String {
            skipSpaces()
            val start = pos
            while (pos < input.length && (input[pos].isLetterOrDigit() || input[pos] == '_')) {
                pos++
            }
            return input.substring(start, pos)
        }

        /** `(10,2)` / `(255)`，读掉即可。 */
        private fun skipParenthesizedArgs() {
            skipSpaces()
            if (peek() != '(') {
                return
            }
            val end = input.indexOf(')', pos)
            require(end > 0) { "括号没有闭合: $input" }
            pos = end + 1
        }

        private fun skipSpaces() {
            while (pos < input.length && input[pos].isWhitespace()) {
                pos++
            }
        }

        private fun peek(): Char? = if (pos < input.length) input[pos] else null

        private fun expect(c: Char) {
            skipSpaces()
            require(peek() == c) { "类型字符串在第 $pos 个字符处应为 '$c'，实际是 '${peek() ?: "结尾"}'（完整类型: $input）" }
            pos++
        }

        fun expectEnd() {
            skipSpaces()
            require(pos >= input.length) { "类型字符串有多余内容: ${input.substring(pos)}（完整类型: $input）" }
        }
    }
}
