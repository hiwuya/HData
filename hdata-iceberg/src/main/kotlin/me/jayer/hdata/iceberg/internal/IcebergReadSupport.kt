package me.jayer.hdata.iceberg.internal

import org.apache.iceberg.FileScanTask

/**
 * When reading directly from data files, position/equality deletes must be handled explicitly; the current parallel
 * reader does not yet implement them, so it fails at the enumeration stage rather than silently reading back deleted
 * rows. The row-limited path that uses Iceberg's GenericReader is not subject to this restriction.
 */
internal fun requireNoDeleteFiles(task: FileScanTask, tableName: String) {
    require(task.deletes().isEmpty()) {
        "Iceberg table [$tableName] contains position/equality delete files, which per-file parallel reads and " +
            "push-down aggregation do not yet support; run compact/rewrite to remove them first, or use limit to " +
            "go through Iceberg's native GenericReader"
    }
}
