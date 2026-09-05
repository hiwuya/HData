package me.jayer.hdata.jdbc.internal

import java.math.BigInteger

/**
 * 表名工具。
 *
 * @author wuya
 * @date 2022-08-04
 */
object TableNames {

    private val RANGE = Regex("""\$\{(\d+)-(\d+)}""")
    private val ONE = BigInteger.ONE

    /** 防止错误配置在构图阶段一次展开数百万张表、直接耗尽 driver 内存。 */
    internal const val MAX_RESOLVED_TABLES = 10_000

    /**
     * 展开分表区间语法：`t_order_${'$'}{00-15}` -> `t_order_00` .. `t_order_15`，
     * 左侧补零宽度跟随区间起点的写法。
     */
    fun resolve(tables: List<String>): List<String> {
        val resolved = ArrayList<String>(minOf(tables.size, MAX_RESOLVED_TABLES))
        tables.forEach { table ->
            val matches = RANGE.findAll(table).toList()
            require(matches.size <= 1) {
                "每个表名最多只能包含一个分表区间: $table"
            }
            val match = matches.singleOrNull()
            if (match == null) {
                require(resolved.size < MAX_RESOLVED_TABLES) {
                    "展开后的表数量不能超过 $MAX_RESOLVED_TABLES"
                }
                resolved.add(table)
                return@forEach
            }

            val padding = match.groupValues[1].length
            val from = match.groupValues[1].toBigInteger()
            val to = match.groupValues[2].toBigInteger()
            require(from <= to) { "非法的表名区间: $table，起点必须 <= 终点，实际 from[$from] > to[$to]" }

            val count = to.subtract(from).add(ONE)
            val available = MAX_RESOLVED_TABLES - resolved.size
            require(count <= BigInteger.valueOf(available.toLong())) {
                "展开后的表数量不能超过 $MAX_RESOLVED_TABLES: $table"
            }
            repeat(count.toInt()) { offset ->
                val index = from.add(BigInteger.valueOf(offset.toLong()))
                resolved.add(RANGE.replaceFirst(table, index.toString().padStart(padding, '0')))
            }
        }
        return resolved
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
