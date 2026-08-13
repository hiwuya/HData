package me.jayer.hdata.jdbc.internal

/**
 * 表名工具。
 *
 * @author wuya
 * @date 2022-08-04
 */
object TableNames {

    private val RANGE = Regex("""\$\{(\d+)-(\d+)}""")

    /**
     * 展开分表区间语法：`t_order_${'$'}{00-15}` -> `t_order_00` .. `t_order_15`，
     * 左侧补零宽度跟随区间起点的写法。
     */
    fun resolve(tables: List<String>): List<String> = tables.flatMap { table ->
        val match = RANGE.find(table) ?: return@flatMap listOf(table)
        val padding = match.groupValues[1].length
        val from = match.groupValues[1].toInt()
        val to = match.groupValues[2].toInt()
        require(from <= to) { "非法的表名区间: $table，起点必须 <= 终点，实际 from[$from] > to[$to]" }
        (from..to).map { index -> RANGE.replace(table, index.toString().padStart(padding, '0')) }
    }

    /** `db.t_order` -> `db` to `t_order`；没有限定符时 schema 为 null。 */
    fun split(table: String): Pair<String?, String> {
        val separator = table.lastIndexOf('.')
        return if (separator < 0) {
            null to table
        } else {
            table.substring(0, separator).trim('`', '"') to table.substring(separator + 1).trim('`', '"')
        }
    }
}
