package cn.edu.whu.oge

import cn.edu.whu.oge.coverage.CoordinateTransformer.projTileCodeToGeoExtent
import cn.edu.whu.oge.coverage.{generateVisTilesByPipeline, generateVisTilesByPipelineV2, generateVisTilesByShuffle, generateVisTilesByShuffleV2, geoExtent}
import geotrellis.layer.SpatialKey
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.Extent

object Main {
  final val TEST_CODES_1 = Array(Array(
    (671, 425), (671, 426), (671, 427), (671, 428), (671, 429),
    (672, 425), (672, 426), (672, 427), (672, 428), (672, 429),
    (673, 425), (673, 426), (673, 427), (673, 428), (673, 429),
    (674, 425), (674, 426), (674, 427), (674, 428), (674, 429)))
  final val TEST_CODES_2 = Array(
    Array(
      (1680, 842), (1681, 842),
      (1680, 844), (1681, 844)
    ), Array(
      (1679, 841), (1680, 841), (1681, 841), (1682, 841),
      (1679, 842), (1680, 842), (1681, 842), (1682, 842),
      (1679, 843), (1680, 843), (1681, 843), (1682, 843),
      (1679, 844), (1680, 844), (1681, 844), (1682, 844)
    ), Array(
      (1677, 839), (1678, 839), (1679, 839), (1680, 839), (1681, 839), (1682, 839), (1683, 839), (1684, 839),
      (1677, 840), (1678, 840), (1679, 840), (1680, 840), (1681, 840), (1682, 840), (1683, 840), (1684, 840),
      (1677, 841), (1678, 841), (1679, 841), (1680, 841), (1681, 841), (1682, 841), (1683, 841), (1684, 841),
      (1677, 842), (1678, 842), (1679, 842), (1680, 842), (1681, 842), (1682, 842), (1683, 842), (1684, 842),
      (1677, 843), (1678, 843), (1679, 843), (1680, 843), (1681, 843), (1682, 843), (1683, 843), (1684, 843),
      (1677, 844), (1678, 844), (1679, 844), (1680, 844), (1681, 844), (1682, 844), (1683, 844), (1684, 844),
      (1677, 845), (1678, 845), (1679, 845), (1680, 845), (1681, 845), (1682, 845), (1683, 845), (1684, 845),
      (1677, 846), (1678, 846), (1679, 846), (1680, 846), (1681, 846), (1682, 846), (1683, 846), (1684, 846)
    ), Array(
      (1675, 837), (1676, 837), (1677, 837), (1678, 837), (1679, 837), (1680, 837), (1681, 837), (1682, 837), (1683, 837), (1684, 837), (1685, 837), (1686, 837),
      (1675, 838), (1676, 838), (1677, 838), (1678, 838), (1679, 838), (1680, 838), (1681, 838), (1682, 838), (1683, 838), (1684, 838), (1685, 838), (1686, 838),
      (1675, 839), (1676, 839), (1677, 839), (1678, 839), (1679, 839), (1680, 839), (1681, 839), (1682, 839), (1683, 839), (1684, 839), (1685, 839), (1686, 839),
      (1675, 840), (1676, 840), (1677, 840), (1678, 840), (1679, 840), (1680, 840), (1681, 840), (1682, 840), (1683, 840), (1684, 840), (1685, 840), (1686, 840),
      (1675, 841), (1676, 841), (1677, 841), (1678, 841), (1679, 841), (1680, 841), (1681, 841), (1682, 841), (1683, 841), (1684, 841), (1685, 841), (1686, 841),
      (1675, 842), (1676, 842), (1677, 842), (1678, 842), (1679, 842), (1680, 842), (1681, 842), (1682, 842), (1683, 842), (1684, 842), (1685, 842), (1686, 842),
      (1675, 843), (1676, 843), (1677, 843), (1678, 843), (1679, 843), (1680, 843), (1681, 843), (1682, 843), (1683, 843), (1684, 843), (1685, 843), (1686, 843),
      (1675, 844), (1676, 844), (1677, 844), (1678, 844), (1679, 844), (1680, 844), (1681, 844), (1682, 844), (1683, 844), (1684, 844), (1685, 844), (1686, 844),
      (1675, 845), (1676, 845), (1677, 845), (1678, 845), (1679, 845), (1680, 845), (1681, 845), (1682, 845), (1683, 845), (1684, 845), (1685, 845), (1686, 845),
      (1675, 846), (1676, 846), (1677, 846), (1678, 846), (1679, 846), (1680, 846), (1681, 846), (1682, 846), (1683, 846), (1684, 846), (1685, 846), (1686, 846),
      (1675, 847), (1676, 847), (1677, 847), (1678, 847), (1679, 847), (1680, 847), (1681, 847), (1682, 847), (1683, 847), (1684, 847), (1685, 847), (1686, 847),
      (1675, 848), (1676, 848), (1677, 848), (1678, 848), (1679, 848), (1680, 848), (1681, 848), (1682, 848), (1683, 848), (1684, 848), (1685, 848), (1686, 848)
    ), Array(
      (836, 418), (836, 419), (836, 420), (836, 421), (836, 422), (836, 423), (836, 424), (836, 425),
      (837, 417), (837, 418), (837, 419), (837, 420), (837, 421), (837, 422), (837, 423), (837, 424), (837, 425),
      (838, 417), (838, 418), (838, 419), (838, 420), (838, 421), (838, 422), (838, 423), (838, 424), (838, 425),
      (839, 417), (839, 418), (839, 419), (839, 420), (839, 421), (839, 422), (839, 423), (839, 424), (839, 425),
      (840, 417), (840, 418), (840, 419), (840, 420), (840, 421), (840, 422), (840, 423), (840, 424), (840, 425),
      (841, 417), (841, 418), (841, 419), (841, 420), (841, 421), (841, 422), (841, 423), (841, 424), (841, 425),
      (842, 417), (842, 418), (842, 419), (842, 420), (842, 421), (842, 422), (842, 423), (842, 424), (842, 425),
      (843, 417), (843, 418), (843, 419), (843, 420), (843, 421), (843, 422), (843, 423), (843, 424), (843, 425),
      (844, 417), (844, 418), (844, 419), (844, 420), (844, 421), (844, 422), (844, 423), (844, 424), (844, 425)
    ))
  final val TEST_ZOOMS_1 = Array(11, 11, 11, 11, 10)
  final val TEST_ZOOMS_2 = Array(11, 11, 11, 11, 10)

  var geoExtents1: Array[Extent] = _
  var geoExtents2: Array[Extent] = _

  def main(args: Array[String]): Unit = {
    geoExtents1 = computeGeoExtents(TEST_CODES_1, TEST_ZOOMS_1)
    geoExtents2 = computeGeoExtents(TEST_CODES_2, TEST_ZOOMS_2)
    // 加载配置
    if (args.length == 0) {
      loadConf()
    } else { // args: 环境，测试方法，数据（单/多波段），数据规模编号
      loadConf(args(0))
      val index = args(3).toInt
      val start = System.currentTimeMillis
      val rdd = if (args(1) == "11") {
        testM1(args(2), index)
      } else if (args(1) == "12") {
        testM1V2(args(2), index)
      } else if (args(1) == "21") {
        testM2(args(2), index)
      } else {
        testM2V2(args(2), index)
      }
//      rdd.persist
      println(s"瓦片总数：${rdd.count}")
      println(s"瓦片范围：${rdd.metadata.extent}")
      println(s"Key范围：${rdd.metadata.bounds}")
      println(s"总耗时：${System.currentTimeMillis - start}")
//      rdd
//        .map(_._1)
//        .collect
//        .sortBy(_.row)
//        .groupBy(_.row)
//        .foreach { case (row, keys) =>
//          println(s"row: $row, col: ${keys.sortBy(_.col).map(_.col).mkString("[", ", ", "]")}")
//        }
//      rdd.foreach { case (key, multibandTile) =>
//        multibandTile.bands(0).renderJpg.write(s"C:\\Users\\DELL\\Desktop\\tile_ans\\mtest1\\${key.col}-${key.row}.jpg")
//      }
    }

    sc.stop
  }

  def computeGeoExtents(codes: Array[Array[(Int, Int)]], zooms: Array[Int]): Array[Extent] = {
    val extents = new Array[Extent](codes.length)
    codes.zipWithIndex.foreach { case (arr, i) =>
      val xCodes = arr.map(_._1)
      val yCodes = arr.map(_._2)
      val leftBottom = projTileCodeToGeoExtent(SpatialKey(xCodes.min, yCodes.max), zooms(i))
      val rightTop = projTileCodeToGeoExtent(SpatialKey(xCodes.max, yCodes.min), zooms(i))
      extents(i) = Extent(leftBottom.xmin, leftBottom.ymin, rightTop.xmax, rightTop.ymax)
    }
    extents
  }

  def testM1(data: String, index: Int): MultibandTileLayerRDD[SpatialKey] =
    if (data == "multi") {
      generateVisTilesByPipeline(sc, TEST_CODES_2(index), "LC81220392015275LGN00", "LC08_L1T", TEST_ZOOMS_2(index))
    } else {
      generateVisTilesByPipeline(sc, TEST_CODES_1(index), "ASTGTM_N28E056", "ASTER_GDEM_DEM30", TEST_ZOOMS_1(index))
    }


  def testM1V2(data: String, index: Int): MultibandTileLayerRDD[SpatialKey] =
    if (data == "multi") {
      generateVisTilesByPipelineV2(sc, TEST_CODES_2(index), "LC81220392015275LGN00", "LC08_L1T", TEST_ZOOMS_2(index))
    } else {
      generateVisTilesByPipelineV2(sc, TEST_CODES_1(index), "ASTGTM_N28E056", "ASTER_GDEM_DEM30", TEST_ZOOMS_1(index))
    }


  def testM2(data: String, index: Int): MultibandTileLayerRDD[SpatialKey] =
    if (data == "multi") {
      geoExtent = geoExtents2(index)
      generateVisTilesByShuffle(sc, TEST_CODES_2(index), "LC81220392015275LGN00", "LC08_L1T", TEST_ZOOMS_2(index))
    } else {
      geoExtent = geoExtents1(index)
      generateVisTilesByShuffle(sc, TEST_CODES_1(index), "ASTGTM_N28E056", "ASTER_GDEM_DEM30", TEST_ZOOMS_1(index))
    }

  def testM2V2(data: String, index: Int): MultibandTileLayerRDD[SpatialKey] =
    if (data == "multi") {
      geoExtent = geoExtents2(index)
      generateVisTilesByShuffleV2(sc, TEST_CODES_2(index), "LC81220392015275LGN00", "LC08_L1T", TEST_ZOOMS_2(index))
    } else {
      geoExtent = geoExtents1(index)
      generateVisTilesByShuffleV2(sc, TEST_CODES_1(index), "ASTGTM_N28E056", "ASTER_GDEM_DEM30", TEST_ZOOMS_1(index))
    }
}
