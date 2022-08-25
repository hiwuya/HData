package io.jayer.hdata.jdbc.statement

import java.io.Serializable

/**
 * @author wuya
 * @date 2022-08-25
 */
interface Statement : Serializable {

    fun buildSql(): String

}