package me.jayer.hdata.hive.format

import java.util.Random

/**
 * 为一个文件位置派生稳定、彼此去相关的随机种子。
 *
 * 不能让每个 SDF restriction 都直接 `Random(userSeed)`：当一个 restriction 只有一个存储块时，
 * 所有块都会取随机序列里的第一个值，最终要么全留、要么全丢。把文件与块偏移混入后，决定不再依赖
 * restriction 如何切分，runner 动态拆分或重试也不会改变同一个块的采样结果。
 */
internal fun samplingSeed(seed: Long, filePath: String, offset: Long): Long {
    var mixed = seed xor offset.rotateLeft(21) xor filePath.hashCode().toLong().rotateLeft(42)
    // SplitMix64 的 avalanche；这里只做确定性混合，不维护全局随机状态。
    mixed = (mixed xor (mixed ushr 30)) * -4658895280553007687L
    mixed = (mixed xor (mixed ushr 27)) * -7723592293110705685L
    return mixed xor (mixed ushr 31)
}

internal fun sampleBlock(seed: Long, filePath: String, offset: Long): Double =
    Random(samplingSeed(seed, filePath, offset)).nextDouble()
