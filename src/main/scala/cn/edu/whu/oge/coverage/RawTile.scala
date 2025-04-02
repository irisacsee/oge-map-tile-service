package cn.edu.whu.oge.coverage

import geotrellis.layer.SpatialKey
import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.vector.Extent
import oge.conf.coverage.OGEDataType.OGEDataType

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

case class RawTile(var path: String,
                   var time: LocalDateTime,
                   var measurement: String,
                   var measurementRank: Int,
                   var coverageId: String,
                   var extent: Extent,
                   var spatialKey: SpatialKey,
                   var offset: Long,
                   var byteCount: Long,
                   var rotation: Double,
                   var resolutionCol: Double,
                   var resolutionRow: Double,
                   var crs: CRS,
                   var dataType: OGEDataType,
                   var tile: Tile,
                   var projCodes: ArrayBuffer[SpatialKey])

case class RawMultibandTile(var tileArrays: Vector[Array[Int]],
                            var extent: Extent,
                            var crs: CRS,
                            var projCodes: ArrayBuffer[SpatialKey])

case class RawMultibandTileInCompute[MR](var mrArrays: Vector[Array[MR]],
                                         var extent: Extent,
                                         var crs: CRS,
                                         var projCodes: ArrayBuffer[SpatialKey])