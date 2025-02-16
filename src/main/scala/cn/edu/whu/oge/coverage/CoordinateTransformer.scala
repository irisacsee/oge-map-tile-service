package cn.edu.whu.oge.coverage

import geotrellis.layer.{LayoutDefinition, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.{LatLng, Transform, WebMercator}
import geotrellis.vector.Extent

/**
 * 各种坐标转换函数
 *
 * @author irisacsee
 * @since 2025/2/16
 */
object CoordinateTransformer {
  final val GEO_TO_PROJ = Transform(LatLng, WebMercator)
  final val PROJ_TO_GEO = Transform(WebMercator, LatLng)
  final val LAYOUT_SCHEME = ZoomedLayoutScheme(WebMercator, tileSize = 256)
  final val LAYOUTS = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    .map(zoom => LAYOUT_SCHEME.levelForZoom(zoom).layout)

  /**
   * WebMercator投影地图瓦片号转地理坐标范围
   *
   * @param projTileCode WebMercator投影地图瓦片号
   * @param zoom         WebMercator投影地图瓦片所在层级
   * @return 地理坐标范围
   */
  def projTileCodeToGeoExtent(projTileCode: SpatialKey,
                              zoom: Int): Extent = {
    val Extent(xmin, ymin, xmax, ymax) = projTileCode.extent(LAYOUTS(zoom))
    val (lngMin, latMin) = PROJ_TO_GEO(xmin, ymin)
    val (lngMax, latMax) = PROJ_TO_GEO(xmax, ymax)
    Extent(lngMin, latMin, lngMax, latMax)
  }
}
