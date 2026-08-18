package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergWriteConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.schemaOf
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory

/**
 * `write_mode: overwrite` 用的一次性清表。
 *
 * 为什么要独立成一步：写入端每个 bundle 各自 append 一个数据文件并提交，
 * 清表如果放进 [IcebergWriteFn]，第二个 bundle 就会把第一个 bundle 刚写进去的数据删掉——
 * 结果是"覆盖"只留下最后一个 bundle 的内容，而作业状态还是成功。
 *
 * 排在所有写入之前靠的是 **side input**：带 side input 的 ParDo 在 side input 那条
 * PCollection 完全算完之前不会处理任何主输入，这是 Beam 对有界数据的保证，
 * 也是标准的"先做 A 再做 B"写法（见 [me.jayer.hdata.iceberg.IcebergWriteProvider]）。
 *
 * 清表本身是幂等的（空表上再删一次什么也不会发生），所以 bundle 重试是安全的。
 *
 * @author wuya
 */
// 触发元素用 String 而不是 Int：Kotlin 的非空 Int 参数会编译成基本类型 int，
// 而 Create.of 产出的是 java.lang.Integer，Beam 反射比对 DoFn 类型时会直接报
// "Type of @Element must match the DoFn type"
class IcebergTruncateFn(private val config: IcebergWriteConfig) : DoFn<String, String>() {

    @ProcessElement
    fun processElement(@Element element: String, receiver: OutputReceiver<String>) {
        IcebergCatalogs.openCatalog(config.warehouse, config.catalogName).use { catalog ->
            val table = IcebergCatalogs.ensureTable(catalog, config.table, schemaOf(config.schemaFields))
            IcebergCatalogs.truncate(table)
            LOGGER.info("write_mode=overwrite: 已清空 Iceberg 表 {}", config.table)
        }
        receiver.output(element)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(IcebergTruncateFn::class.java)
    }
}
