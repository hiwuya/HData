package io.jayer.hdata.jdbc.type

import java.io.Serializable

/**
 * @author wuya
 * @date 2022-08-25
 */
fun interface FieldValueConverter : Serializable {
    fun convert(value: Any): Any
}