package cn.edu.whu.oge

import cn.edu.whu.oge.coverage.COGParser.{TILE_BYTE_COUNT, cogTileQueryForM1, cogTileQueryForM2, getCOGTileByRawTile, getCOGTileByRawTileArray, getCOGTileBytes, getCOGTilesMetaForM1}
import cn.edu.whu.oge.coverage.CoordinateTransformer.{GEO_TO_PROJ, LAYOUTS, LAYOUT_SCHEME, projTileCodeToGeoExtent}
import cn.edu.whu.oge.coverage.TileSerializer.deserializeTileData
import cn.edu.whu.oge.obs.getMinioClient
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng, Transform, WebMercator}
import geotrellis.raster.reproject.{RasterRegionReproject, Reproject}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{ArrayTile, CellSize, CellType, GridBounds, IntCellType, MultibandTile, Raster, RasterExtent, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.vector.{Extent, Polygon, ProjectedExtent}
import oge.conf.coverage.CoverageMetadata
import oge.conf.coverage.CoverageMetadata.queryCoverage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.time.ZoneOffset
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.{max, min}

/**
 * coverage类型数据的算子包
 *
 * @author irisacsee
 * @since 2025/2/14
 */
package object coverage {
  var geoExtent: Extent = _
  var partitionNum: Int = 16 // Spark任务分区数
//  var computeStart: Long = System.currentTimeMillis

  /**
   * 每个可视瓦片的生成为独立的任务
   *
   * @param sc            SparkContext
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByPipeline(implicit sc: SparkContext,
                                 projTileCodes: Array[(Int, Int)],
                                 coverageId: String,
                                 productKey: String,
                                 zoom: Int): MultibandTileLayerRDD[SpatialKey] = {
    // 瓦片号
    var metaList = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    } else if (metaList.size > 1) {
      metaList = metaList.take(3)  // 取前三个波段
      metaList.foreach(meta => println(meta.measurement))
    }
    val queryGeometry = metaList.head.geom

    // 头部解析
    val time1 = System.currentTimeMillis
    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)
    val tilesMeta = tileMetadata
      .mapPartitions(par => {
        val obs = getMinioClient
        par.map(coverageMetadata =>
          (s"${coverageMetadata.coverageId}-${coverageMetadata.measurement}",
            cogTileQueryForM1(obs, zoom, coverageMetadata)))
      })
      .collect
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    val tileCodesWithId = tilesMeta.flatMap { case (key, meta) =>
      projTileCodes.map(code => (code, meta))
    }
    println(s"codes size: ${tileCodesWithId.length}")

    // 找到每个TMS瓦片需要的COG瓦片，然后生成TMS瓦片
    val layout = LAYOUTS(zoom)
    val tileRdd = sc.parallelize(tileCodesWithId, partitionNum)
      .mapPartitions(par => {
        val obs = getMinioClient
//        val time2 = System.currentTimeMillis
//        println(s"分区内计算开始，起始时间为$time2")
        val ans = par.map { case ((xCode, yCode),
        (coverageMetadata, tileByteCounts, cell, geoTrans, tileOffsets, bandCount)) =>
          // 根据code在meta中找到需要的COG瓦片
          val extent = projTileCodeToGeoExtent(SpatialKey(xCode, yCode), zoom)
          val cogTileMetaArray = getCOGTilesMetaForM1(
            zoom, coverageMetadata, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, extent, queryGeometry)
          // 读取COG瓦片
          val time4 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片开始读取所需的COG瓦片，当前时间为$time4")
          val cogTileArray = cogTileMetaArray.map { case (offset, byteCount, dataType, innerExtent, spatialKey) =>
            val tileBytes = getCOGTileBytes(obs, coverageMetadata.path, offset, byteCount)
            val tile = (deserializeTileData("", tileBytes, 256, dataType), innerExtent)
            //  tile._1.renderJpg.write(s"C:\\Users\\DELL\\Desktop\\tile_ans\\cog1\\${spatialKey.col}-${spatialKey.row}.jpg")
            tile
          }
          val time5 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片读取所需的COG瓦片结束，结束时间为$time5，耗时：${time5 - time4}ms")
          // 计算
          val time6 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片开始计算，当前时间为$time6")
          val trans = Transform(CRS.fromEpsgCode(4326), WebMercator)
          val (xMin, yMin) = trans(extent.xmin, extent.ymin)
          val (xMax, yMax) = trans(extent.xmax, extent.ymax)
          val destExtent = Extent(xMin, yMin, xMax, yMax)
          val destRaster: Raster[Tile] = Raster(ArrayTile.empty(IntCellType, 256, 256), destExtent)
          val rrp = implicitly[RasterRegionReproject[Tile]]
          cogTileArray.foreach { case (tile, innerExtent) =>
            // TODO 投影不一致时需要考虑gridBounds的计算
            val gridBounds = GridBounds(0, 0, tile.cols - 1, tile.rows - 1)
            val innerRasterExtent = RasterExtent(innerExtent, gridBounds.width, gridBounds.height)
            val outerGridBounds =
              GridBounds(
                -gridBounds.colMin,
                -gridBounds.rowMin,
                tile.cols - gridBounds.colMin - 1,
                tile.rows - gridBounds.rowMin - 1
              )
            val outerExtent = innerRasterExtent.extentFor(outerGridBounds, clamp = false)
            val raster = Raster(tile, outerExtent)
            rrp.regionReprojectMutable(
              raster, coverageMetadata.crs, WebMercator, destRaster,
              ProjectedExtent(innerExtent, coverageMetadata.crs).reprojectAsPolygon(WebMercator, 0.05),
              ResampleMethod.DEFAULT, 0.05)
          }
          val time7 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片计算结束，结束时间为$time7，耗时：${time7 - time6}ms")
          (SpatialKey(xCode, yCode), (coverageMetadata.measurementRank, destRaster.tile))
        }
//        val time3 = System.currentTimeMillis
//        println(s"分区内计算结束，结束时间为$time3，耗时: ${time3 - time2}ms")
        ans
      })
    val multibandTileRdd = tileRdd.groupByKey.map { case (spatialKey, iter) =>
      (spatialKey, MultibandTile(iter.toArray.sortBy(_._1).map(_._2)))
    }
    MultibandTileLayerRDD(multibandTileRdd, collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 每个可视瓦片的生成为独立的任务，但在分区内优化了数据的读取
   *
   * @param sc            SparkContext
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByPipelineV2(implicit sc: SparkContext,
                                   projTileCodes: Array[(Int, Int)],
                                   coverageId: String,
                                   productKey: String,
                                   zoom: Int): MultibandTileLayerRDD[SpatialKey] = {
    // 瓦片号
    var metaList = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    } else if (metaList.size > 1) {
      metaList = metaList.take(3)  // 取前三个波段
      metaList.foreach(meta => println(meta.measurement))
    }
    val queryGeometry = metaList.head.geom

    // 头部解析
    val time1 = System.currentTimeMillis
    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)
    val tilesMeta = tileMetadata
      .mapPartitions(par => {
        val obs = getMinioClient
        par.map(coverageMetadata =>
          (s"${coverageMetadata.coverageId}-${coverageMetadata.measurement}",
            cogTileQueryForM1(obs, zoom, coverageMetadata)))
      })
      .collect
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    val tileCodesWithId = tilesMeta.flatMap { case (key, meta) =>
      projTileCodes.map(code => (code, meta))
    }
    println(s"codes size: ${tileCodesWithId.length}")

    // 找到每个TMS瓦片需要的COG瓦片，然后生成TMS瓦片
    val layout = LAYOUTS(zoom)
    val tileRdd = sc.parallelize(tileCodesWithId, partitionNum)
      .mapPartitions(par => {
        val obs = getMinioClient
//        val time2 = System.currentTimeMillis
//        println(s"分区内计算开始，起始时间为$time2")
        val map = mutable.HashMap[String, Tile]()
        val ans = par.map { case ((xCode, yCode),
        (coverageMetadata, tileByteCounts, cell, geoTrans, tileOffsets, bandCount)) =>
          // 根据code在meta中找到需要的COG瓦片
          val visTileGeoExtent = projTileCodeToGeoExtent(SpatialKey(xCode, yCode), zoom)
          val cogTileMetaArray = getCOGTilesMetaForM1(
            zoom, coverageMetadata, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, visTileGeoExtent, queryGeometry)
          // 读取COG瓦片
          val time4 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片开始读取所需的COG瓦片，当前时间为$time4")
          val cogTileArray = cogTileMetaArray.map { case (offset, byteCount, dataType, innerExtent, spatialKey) =>
            val key = s"${coverageMetadata.path}-$offset"
            val tile = if (map.contains(key)) {
              map(key)
            } else {
              val tileBytes = getCOGTileBytes(obs, coverageMetadata.path, offset, byteCount)
              val t = deserializeTileData("", tileBytes, 256, dataType)
              map += (key -> t)
              t
            }
            //            tile._1.renderJpg.write(s"C:\\Users\\DELL\\Desktop\\tile_ans\\cog1\\${spatialKey.col}-${spatialKey.row}.jpg")
            (tile, innerExtent)
          }
          val time5 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片读取所需的COG瓦片结束，结束时间为$time5，耗时：${time5 - time4}ms")
          // 计算
          val time6 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片开始计算，当前时间为$time6")
          val (xMin, yMin) = GEO_TO_PROJ(visTileGeoExtent.xmin, visTileGeoExtent.ymin)
          val (xMax, yMax) = GEO_TO_PROJ(visTileGeoExtent.xmax, visTileGeoExtent.ymax)
          val destExtent = Extent(xMin, yMin, xMax, yMax)
          val destRaster: Raster[Tile] = Raster(ArrayTile.empty(IntCellType, 256, 256), destExtent)
          val rrp = implicitly[RasterRegionReproject[Tile]]
          cogTileArray.foreach { case (tile, innerExtent) =>
            println(s"innerExtent: $innerExtent")
            println(s"coverageMetaCrs: ${coverageMetadata.crs}")
            // TODO 投影不一致时需要考虑gridBounds的计算
            val gridBounds = GridBounds(0, 0, tile.cols - 1, tile.rows - 1)
            val innerRasterExtent = RasterExtent(innerExtent, gridBounds.width, gridBounds.height)
            val outerGridBounds =
              GridBounds(
                -gridBounds.colMin,
                -gridBounds.rowMin,
                tile.cols - gridBounds.colMin - 1,
                tile.rows - gridBounds.rowMin - 1
              )
            val outerExtent = innerRasterExtent.extentFor(outerGridBounds, clamp = false)
            val raster = Raster(tile, outerExtent)
            rrp.regionReprojectMutable(
              raster, coverageMetadata.crs, WebMercator, destRaster,
              ProjectedExtent(innerExtent, coverageMetadata.crs).reprojectAsPolygon(WebMercator, 0.05),
              ResampleMethod.DEFAULT, 0.05)
          }
          val time7 = System.currentTimeMillis
          println(s"$xCode-${yCode}瓦片计算结束，结束时间为$time7，耗时：${time7 - time6}ms")
          (SpatialKey(xCode, yCode), (coverageMetadata.measurementRank, destRaster.tile))
        }
//        val time3 = System.currentTimeMillis
//        println(s"分区内计算结束，结束时间为$time3，耗时: ${time3 - time2}ms")
        ans
      })
    val multibandTileRdd = tileRdd.groupByKey.map { case (spatialKey, iter) =>
      (spatialKey, MultibandTile(iter.toArray.sortBy(_._1).map(_._2)))
    }
    MultibandTileLayerRDD(multibandTileRdd, collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 每个COG瓦片只读一次，生成可视瓦片需要Shuffle
   *
   * @param sc         SparkContext
   * @param coverageId 影像ID
   * @param productKey 产品号
   * @param zoom       前端地图层级
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByShuffle(implicit sc: SparkContext,
                                projTileCodes: Array[(Int, Int)],
                                coverageId: String,
                                productKey: String,
                                zoom: Int): MultibandTileLayerRDD[SpatialKey] = {
    var metaList = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    } else if (metaList.size > 1) {
      metaList = metaList.take(3)  // 取前三个波段
      metaList.foreach(meta => println(meta.measurement))
    }

    //    val union = if (Trigger.windowExtent == null) {
    //      Array(Extent(metaList.head.geom.getEnvelopeInternal))
    //    } else {
    //      Array(Trigger.windowExtent)
    //    }
//    val union = Array(geoExtent)
    val crs = metaList.head.crs
    val queryGeometry = metaList.head.geom
    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)

    val time1 = System.currentTimeMillis
    val tileRddFlat = tileMetadata
      .mapPartitions(par => {
        val obs = getMinioClient
        par.map(t => { // 合并所有的元数据（追加了范围）
          val rawTiles: ArrayBuffer[RawTile] = cogTileQueryForM2(obs, zoom, t, projTileCodes, queryGeometry)
          println(s"")
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
      }).flatMap(t => t).persist
    val tileNum: Int = tileRddFlat.count.toInt
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    println("tileNum = " + tileNum)
    if (tileNum <= 0) {
      throw new Exception("There are no tiles within the visible range!")
    }

    val tileRddRePar: RDD[RawTile] = tileRddFlat.repartition(math.min(tileNum, partitionNum))
    tileRddFlat.unpersist()
    val rawTileRdd: RDD[RawTile] = tileRddRePar.mapPartitions(par => {
      val obs = getMinioClient
      par.map(t => {
        val time2 = System.currentTimeMillis
        println(s"${t.path}于${t.offset}处瓦片开始读取，起始时间为$time2")
        getCOGTileByRawTile(obs, t)
        val time3 = System.currentTimeMillis
        println(s"${t.path}于${t.offset}处瓦片读取结束，但未反序列化，结束时间为$time3，耗时：${time3 - time2}ms")
        t
      })
    })
    //    println("Loading data Time: " + (System.currentTimeMillis() - start))
    val coverage = reprojectByShuffle(rawTileRdd, zoom, crs)
    MultibandTileLayerRDD(coverage, collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 每个COG瓦片只读一次，生成可视瓦片需要Shuffle，对各分区内的数据读取做了优化
   *
   * @param sc         SparkContext
   * @param coverageId 影像ID
   * @param productKey 产品号
   * @param zoom       前端地图层级
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByShuffleV2(implicit sc: SparkContext,
                                  projTileCodes: Array[(Int, Int)],
                                  coverageId: String,
                                  productKey: String,
                                  zoom: Int): MultibandTileLayerRDD[SpatialKey] = {
    var metaList = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    } else if (metaList.size > 1) {
      metaList = metaList.take(3)  // 取前三个波段
      metaList.foreach(meta => println(meta.measurement))
    }

    //    val union = if (Trigger.windowExtent == null) {
    //      Array(Extent(metaList.head.geom.getEnvelopeInternal))
    //    } else {
    //      Array(Trigger.windowExtent)
    //    }
//    val union = Array(geoExtent)
//    println(union(0))
    val crs = metaList.head.crs
    val queryGeometry = metaList.head.geom
    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)

    val time1 = System.currentTimeMillis
    val tileRddFlat = tileMetadata
      .mapPartitions(par => {
        val obs = getMinioClient
        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
          val time1: Long = System.currentTimeMillis()
          val rawTiles = cogTileQueryForM2(obs, zoom, t, projTileCodes, queryGeometry)
          val time2: Long = System.currentTimeMillis()
          println("Get Tiles Meta Time is " + (time2 - time1))
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
        result
      }).flatMap(t => t).collect
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    val tileNum: Int = tileRddFlat.length
    println("tileNum = " + tileNum)
    if (tileNum <= 0) {
      throw new Exception("There are no tiles within the visible range!")
    }
    val tileRDDRePar: RDD[RawTile] = sc.parallelize(tileRddFlat, math.min(tileNum, partitionNum))
    val rawTileRdd: RDD[RawTile] = tileRDDRePar.mapPartitions(par => {
      val obs = getMinioClient
      val map = mutable.Map[String, ArrayBuffer[RawTile]]()
      // TODO: 同一path可能存在不相邻的tile
      par.foreach(t => {
        if (map.contains(t.path)) {
          map(t.path) += t
        } else {
          val ab = ArrayBuffer[RawTile]()
          ab += t
          map.put(t.path, ab)
        }
      })
      map
        .flatMap { case (path, ab) =>
          val startPos = ab.head.offset
          val byteCount = ab.last.offset - startPos + TILE_BYTE_COUNT
          val time1 = System.currentTimeMillis
          println(s"${ab.head.path}于${startPos}处瓦片集合开始读取，共读取${byteCount}字节，起始时间为$time1")
          getCOGTileByRawTileArray(obs, ab, path, startPos, byteCount)
          val time2 = System.currentTimeMillis
          println(s"${ab.head.path}于${startPos}处瓦片集合读取结束，结束时间为$time2，耗时：${time2 - time1}ms")
          ab
        }
        .iterator
    }).persist
    val coverage = reprojectByShuffle(rawTileRdd, zoom, crs)
    MultibandTileLayerRDD(coverage, collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 从投影瓦片号中获取元数据
   *
   * @param projTileCodes 投影瓦片号
   * @param zoom          当前层级
   * @return 元数据
   */
  private def collectMetadataFromProjTileCodes(projTileCodes: Array[(Int, Int)],
                                               zoom: Int): TileLayerMetadata[SpatialKey] = {
    val xCodes = projTileCodes.map(_._1)
    val yCodes = projTileCodes.map(_._2)
    val (minXCode, maxXCode) = (xCodes.min, xCodes.max)
    val (minYCode, maxYCode) = (yCodes.min, yCodes.max)
    val leftBottom = SpatialKey(minXCode, maxYCode).extent(LAYOUTS(zoom))
    val rightTop = SpatialKey(maxXCode, minYCode).extent(LAYOUTS(zoom))
    TileLayerMetadata(IntCellType, LAYOUTS(zoom),
      Extent(leftBottom.xmin, leftBottom.ymin, rightTop.xmax, rightTop.ymax),
      WebMercator, Bounds(SpatialKey(minXCode, minYCode), SpatialKey(maxXCode, maxYCode)))
  }

  /**
   * 采用Shuffle方式重投影
   *
   * @param rawTileRdd 原始瓦片集合
   * @param zoom       当前层及
   * @param crs        原始坐标信息
   * @return 重投影后的瓦片集合
   */
  private def reprojectByShuffle(rawTileRdd: RDD[RawTile],
                                zoom: Int,
                                crs: CRS): RDD[(SpatialKey, MultibandTile)] = {
    val spaceBandRdd = rawTileRdd.flatMap(rawTile => {
      val (tile, innerExtent) = (rawTile.tile, rawTile.extent)
      val gridBounds = GridBounds(0, 0, tile.cols - 1, tile.rows - 1)
      val innerRasterExtent = RasterExtent(innerExtent, gridBounds.width, gridBounds.height)
      val outerGridBounds =
        GridBounds(
          -gridBounds.colMin,
          -gridBounds.rowMin,
          tile.cols - gridBounds.colMin - 1,
          tile.rows - gridBounds.rowMin - 1
        )
      val outerExtent = innerRasterExtent.extentFor(outerGridBounds, clamp = false)
      val raster = Raster(tile, outerExtent)
      val destRegion = ProjectedExtent(innerExtent, rawTile.crs).reprojectAsPolygon(WebMercator, 0.05)
      val measurementRank = rawTile.measurementRank
      rawTile.projCodes.map(code => {
        val destRE = RasterExtent(code.extent(LAYOUTS(zoom)), 256, 256)
        (SpaceBandKey(code, measurementRank), (raster, destRE, destRegion))
      })
    })

    val rrp = implicitly[RasterRegionReproject[Tile]]
    def createCombiner(tup: (Raster[Tile], RasterExtent, Polygon)) = {
      val (raster, destRE, destRegion) = tup
      rrp.regionReproject(raster, crs, WebMercator, destRE, destRegion, ResampleMethod.DEFAULT, 0.05).tile
    }
    def mergeValues(reprojectedTile: Tile, toReproject: (Raster[Tile], RasterExtent, Polygon)) = {
      val (raster, destRE, destRegion) = toReproject
      val destRaster = Raster(reprojectedTile, destRE.extent)
      rrp.regionReprojectMutable(raster, crs, WebMercator, destRaster, destRegion, ResampleMethod.DEFAULT, 0.05).tile
    }
    def mergeCombiners(reproj1: Tile, reproj2: Tile) = reproj1.merge(reproj2)

    spaceBandRdd
      .combineByKey(createCombiner, mergeValues, mergeCombiners)
      .map { case (spaceBandKey, tile) =>
        (spaceBandKey.spatialKey, (spaceBandKey.measurementRank, tile))
      }
      .groupByKey
      .map { case (spatialKey, iter) =>
        (spatialKey, MultibandTile(iter.toArray.sortBy(_._1).map(_._2)))
      }
  }

  private def mergeLists(resoMeasurementMap: mutable.ListMap[Double, List[String]]):
  mutable.Map[Double, List[String]] = {
    // 定义一个辅助函数，用于合并List[String]
    def mergeListsHelper(lists: List[List[String]]): List[String] = lists.flatten.distinct

    // 使用foldLeft遍历sortedMap，合并相对差距小于万分之一的List[String]
    val mergedMap = resoMeasurementMap.foldLeft(mutable.Map.empty[Double, List[String]]) { case (acc, (key, value)) =>
      acc.lastOption match {
        case Some((prevKey, prevValue)) if math.abs((key - prevKey) / key) < 0.0001 =>
          acc.init + (prevKey -> mergeListsHelper(List(prevValue, value)))
        case _ =>
          acc + (key -> value)
      }
    }
    mergedMap
  }
}
