package cn.edu.whu.oge.coverage

import geotrellis.layer.{Boundable, SpatialKey}

/**
 * 空间与波段排序组合的key
 *
 * @param spatialKey
 * @param measurementRank
 * @author irisacsee
 * @since 2025/2/16
 */
case class SpaceBandKey(spatialKey: SpatialKey, measurementRank: Int)

object SpaceBandKey {
  implicit def ordering: Ordering[SpaceBandKey] =
    Ordering.by(sbk => (sbk.spatialKey, sbk.measurementRank))

  implicit object Boundable extends Boundable[SpaceBandKey] {
    def minBound(a: SpaceBandKey, b: SpaceBandKey): SpaceBandKey = {
      SpaceBandKey(
        SpatialKey(math.min(a.spatialKey.col, b.spatialKey.col), math.min(a.spatialKey.row, b.spatialKey.row)),
        if (a.measurementRank < b.measurementRank) a.measurementRank else b.measurementRank)
    }

    def maxBound(a: SpaceBandKey, b: SpaceBandKey): SpaceBandKey = {
      SpaceBandKey(
        SpatialKey(math.max(a.spatialKey.col, b.spatialKey.col), math.max(a.spatialKey.row, b.spatialKey.row)),
        if (a.measurementRank < b.measurementRank) a.measurementRank else b.measurementRank)
    }
  }
}