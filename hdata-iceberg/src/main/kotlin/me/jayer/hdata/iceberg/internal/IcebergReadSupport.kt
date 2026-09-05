package me.jayer.hdata.iceberg.internal

import org.apache.iceberg.FileScanTask

/**
 * 按数据文件直接读取时必须显式处理 position/equality delete；当前并行读取器尚未实现它们，
 * 因此在枚举阶段失败，不能把已删除的行静默读回来。使用 Iceberg GenericReader 的限行路径不受此限制。
 */
internal fun requireNoDeleteFiles(task: FileScanTask, tableName: String) {
    require(task.deletes().isEmpty()) {
        "Iceberg 表[$tableName]包含 position/equality delete 文件，当前按文件并行读取与聚合下推尚不支持；" +
            "请先 compact/rewrite 删除，或使用 limit 走 Iceberg 原生 GenericReader"
    }
}
