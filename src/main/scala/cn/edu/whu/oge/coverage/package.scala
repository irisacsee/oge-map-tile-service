package cn.edu.whu.oge

import cn.edu.whu.oge.coverage.AspectSlopeOperators.ASMidResult
import cn.edu.whu.oge.coverage.COGParser.{COGTileMeta, TILE_BYTE_COUNT, cogTileQueryForM1, cogTileQueryForM2, getCOGTileByRawTile, getCOGTileByRawTileArray, getCOGTileBytes, getCOGTilesMetaForM1}
import cn.edu.whu.oge.coverage.ComputeInKeyOperators.normalizedDifferenceOp
import cn.edu.whu.oge.coverage.CoordinateTransformer.{GEO_TO_PROJ, LAYOUTS, LAYOUT_SCHEME, projTileCodeToGeoExtent}
import cn.edu.whu.oge.coverage.DEMOperators.DEMWindow
import cn.edu.whu.oge.coverage.TileSerializer.deserializeTileData
import cn.edu.whu.oge.obs.OBSConf.{OBSConfTuple, bucketName}
import cn.edu.whu.oge.obs.{OBSConf, getMinioClient}
import cn.edu.whu.oge.server.CacheManager.{COG_META_CACHE, COG_TILE_META_CACHE}
import geotrellis.layer.{Bounds, KeyBounds, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.reproject.RasterRegionReproject
import geotrellis.raster.resample.{Bilinear, ResampleMethod}
import geotrellis.raster.{ArrayTile, GridBounds, IntArrayTile, IntConstantNoDataCellType, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.{Extent, Polygon, ProjectedExtent}
import oge.conf.coverage.CoverageMetadata
import oge.conf.coverage.CoverageMetadata.queryCoverage
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import spire.ClassTag

import java.io.File
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * coverage类型数据的算子包
 *
 * @author irisacsee
 * @since 2025/2/14
 */
package object coverage {
  type RawMultibandTilesWithMeta = (RDD[(SpatialKey, RawMultibandTile)], (Int, Int, Int, Int), CRS)

  final val WEB_MERCATOR_RESOLUTION: Array[Double] = Array(
    156543.033928, 78271.516964, 39135.758482, 19567.879241, 9783.939621,
    4891.969810, 2445.984905, 1222.992453, 611.496226, 305.748113,
    152.874057, 76.437028, 38.218514, 19.109257, 9.554629,
    4.777314, 2.388657, 1.194329, 0.597164, 0.298582, 0.149291)
  final val TILE_SIZE = 256
  final val TILE_MAX_INDEX = 255
  final val TILE_CELL_COUNT = 256 * 256

  var partitionNum: Int = 16 // Spark任务分区数
  var storePath: String = _

  /**
   * 查询COG元数据
   *
   * @param coverageId 影像ID
   * @param productKey 产品号
   * @param bandNames  要使用的波段名
   * @return COG元数据
   */
  private def cogMetaQuery(coverageId: String,
                           productKey: String,
                           bandNames: Set[String]): ListBuffer[CoverageMetadata] = {
    var metaList = if (COG_META_CACHE.containsKey(coverageId)) COG_META_CACHE.get(coverageId) else {
      val ans = queryCoverage(coverageId, productKey)
      COG_META_CACHE.put(coverageId, ans)
      ans
    }
    println(metaList.head)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    } else if (metaList.size > 1) {
      //      metaList = metaList.take(3)
      metaList = metaList.filter(meta => bandNames.contains(meta.measurement))
      metaList.foreach(meta => println(meta.measurement))
    }
    metaList
  }

  /**
   * 基于正向瓦片匹配，每个可视瓦片的生成为独立的任务
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @param version       读取优化版本标识
   * @return 生成的投影瓦片集合
   */
  private def generateVisTilesByForwardMatch(projTileCodes: Array[(Int, Int)],
                                             coverageId: String,
                                             productKey: String,
                                             zoom: Int,
                                             bandNames: Set[String])
                                            (version: String): MultibandTileLayerRDD[SpatialKey] = {
    // 瓦片号
    val metaList = cogMetaQuery(coverageId, productKey, bandNames)
    val queryGeometry = metaList.head.geom
    val obsConfTuple = OBSConf.toTuple

    def readAndGenerateFunc(): Iterator[((Int, Int), COGTileMeta)] => Iterator[(SpatialKey, (Int, Tile))] = par =>
      if (version == "v1") forwardReadAndGenerateV1(par, obsConfTuple, queryGeometry, zoom)
      else forwardReadAndGenerateV2(par, obsConfTuple, queryGeometry, zoom)

    // 找到每个TMS瓦片需要的COG瓦片，然后生成TMS瓦片
    val tileRdd = sc
      .parallelize(forwardMatch(obsConfTuple, coverageId, metaList, projTileCodes, zoom), partitionNum)
      .mapPartitions(readAndGenerateFunc())
    val multibandTileRdd = tileRdd
      .filter(_ != null)
      .groupByKey
      .map { case (spatialKey, iter) =>
        (spatialKey, MultibandTile(iter.toArray.sortBy(_._1).map(_._2)))
      }
    MultibandTileLayerRDD(multibandTileRdd, collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 正向瓦片匹配
   *
   * @param obsConfTuple  OBS配置元组
   * @param cogMetaList   COG元数据列表
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param zoom          当前可视层级
   * @return 可视瓦片号到COG瓦片元数据的匹配对数组
   */
  private def forwardMatch(obsConfTuple: (String, String, String, String, Int),
                           coverageId: String,
                           cogMetaList: ListBuffer[CoverageMetadata],
                           projTileCodes: Array[(Int, Int)],
                           zoom: Int): Array[((Int, Int), COGTileMeta)] = {
    // 头部解析
    val time1 = System.currentTimeMillis
    val tilesMeta = if (COG_TILE_META_CACHE.containsKey(coverageId)) COG_TILE_META_CACHE.get(coverageId) else {
      val ans = sc
        .makeRDD(cogMetaList)
        .mapPartitions(par => {
          val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
          bucketName = obsConfTuple._4
          par.map(coverageMetadata =>
            (s"${coverageMetadata.coverageId}-${coverageMetadata.measurement}",
              cogTileQueryForM1(obs, zoom, coverageMetadata)))
        })
        .collect
      COG_TILE_META_CACHE.put(coverageId, ans)
      ans
    }
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    val tileCodesWithId = tilesMeta.flatMap { case (key, meta) =>
      projTileCodes.map(code => (code, meta))
    }
    println(s"codes size: ${tileCodesWithId.length}")
    tileCodesWithId
  }

  /**
   * 正向方法中读取与生成的流程，读取未优化版
   *
   * @param par           分区内数据
   * @param obsConfTuple  对象存储配置元组
   * @param queryGeometry COG影像范围
   * @param zoom          当前可视层级
   * @return 生成的瓦片集合，每一条数据为(空间key, (波段排序, 瓦片))
   */
  private def forwardReadAndGenerateV1(par: Iterator[((Int, Int), COGTileMeta)],
                                       obsConfTuple: OBSConfTuple,
                                       queryGeometry: Geometry,
                                       zoom: Int): Iterator[(SpatialKey, (Int, Tile))] = {
    bucketName = obsConfTuple._4
    val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
    val uuid = UUID.randomUUID.toString
    //        val time2 = System.currentTimeMillis
    //        println(s"分区内计算开始，起始时间为$time2")
    val ans = par.map { case ((xCode, yCode),
    (coverageMetadata, tileByteCounts, cell, geoTrans, tileOffsets, bandCount)) =>
      // 根据code在meta中找到需要的COG瓦片
      val visTileGeoExtent = projTileCodeToGeoExtent(SpatialKey(xCode, yCode), zoom)
      val cogTileMetaArray = getCOGTilesMetaForM1(
        zoom, coverageMetadata, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, visTileGeoExtent, queryGeometry)
      // 读取COG瓦片
      //          val time4 = System.currentTimeMillis
      //          println(s"rs，$xCode-${yCode}瓦片开始读取所需的COG瓦片，起始时间为$time4，分区：$uuid")
      if (cogTileMetaArray.nonEmpty) {
        val cogTileArray = cogTileMetaArray.map { case (offset, byteCount, dataType, innerExtent, spatialKey) =>
          val tileBytes = getCOGTileBytes(obs, coverageMetadata.path, offset, byteCount)
          val tile = (deserializeTileData("", tileBytes, 256, dataType), innerExtent)
          //  tile._1.renderJpg.write(s"C:\\Users\\DELL\\Desktop\\tile_ans\\cog1\\${spatialKey.col}-${spatialKey.row}.jpg")
          tile
        }
        //          val time5 = System.currentTimeMillis
        //          println(s"re，$xCode-${yCode}瓦片读取所需的COG瓦片结束，结束时间为$time5，耗时：${time5 - time4}ms，分区：$uuid")
        // 计算
        //          val time6 = System.currentTimeMillis
        //          println(s"$xCode-${yCode}瓦片开始计算，当前时间为$time6")
        val destRaster = reproject(cogTileArray, visTileGeoExtent, coverageMetadata.crs)
        //          val time7 = System.currentTimeMillis
        //          println(s"$xCode-${yCode}瓦片计算结束，结束时间为$time7，耗时：${time7 - time6}ms")
        (SpatialKey(xCode, yCode), (coverageMetadata.measurementRank, destRaster.tile))
      } else {
        null
      }
    }
    //        val time3 = System.currentTimeMillis
    //        println(s"分区内计算结束，结束时间为$time3，耗时: ${time3 - time2}ms")
    ans
  }

  /**
   * 正向方法中读取与生成的流程，读取优化版
   *
   * @param par           分区内数据
   * @param obsConfTuple  对象存储配置元组
   * @param queryGeometry COG影像范围
   * @param zoom          当前可视层级
   * @return 生成的瓦片集合，每一条数据为(空间key, (波段排序, 瓦片))
   */
  private def forwardReadAndGenerateV2(par: Iterator[((Int, Int), COGTileMeta)],
                                       obsConfTuple: OBSConfTuple,
                                       queryGeometry: Geometry,
                                       zoom: Int): Iterator[(SpatialKey, (Int, Tile))] = {
    bucketName = obsConfTuple._4
    val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
    //        val time2 = System.currentTimeMillis
    //        println(s"分区内计算开始，起始时间为$time2")
    val uuid = UUID.randomUUID.toString
    val map = mutable.HashMap[String, Tile]()
    val ans = par.map { case ((xCode, yCode),
    (coverageMetadata, tileByteCounts, cell, geoTrans, tileOffsets, bandCount)) =>
      // 根据code在meta中找到需要的COG瓦片
      val visTileGeoExtent = projTileCodeToGeoExtent(SpatialKey(xCode, yCode), zoom)
      val cogTileMetaArray = getCOGTilesMetaForM1(
        zoom, coverageMetadata, tileOffsets, cell, geoTrans,
        tileByteCounts, bandCount, visTileGeoExtent, queryGeometry)
      if (cogTileMetaArray.nonEmpty) {
        // 读取COG瓦片
        val time4 = System.currentTimeMillis
        //          println(s"rs，$xCode-${yCode}瓦片开始读取所需的COG瓦片，起始时间为$time4，分区：$uuid")
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
        //          println(s"re，$xCode-${yCode}瓦片读取所需的COG瓦片结束，结束时间为$time5，耗时：${time5 - time4}ms，分区：$uuid")
        // 计算
        //          val time6 = System.currentTimeMillis
        //          println(s"$xCode-${yCode}瓦片开始计算，当前时间为$time6")
        val destRaster = reproject(cogTileArray, visTileGeoExtent, coverageMetadata.crs)
        //          val time7 = System.currentTimeMillis
        //          println(s"$xCode-${yCode}瓦片计算结束，结束时间为$time7，耗时：${time7 - time6}ms")
        (SpatialKey(xCode, yCode), (coverageMetadata.measurementRank, destRaster.tile))
      } else {
        null
      }
    }
    //        val time3 = System.currentTimeMillis
    //        println(s"分区内计算结束，结束时间为$time3，耗时: ${time3 - time2}ms")
    ans
  }

  /**
   * 读取未优化的正向方法
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByForwardMatchV1(projTileCodes: Array[(Int, Int)],
                                       coverageId: String,
                                       productKey: String,
                                       zoom: Int,
                                       bandNames: Set[String]): MultibandTileLayerRDD[SpatialKey] =
    generateVisTilesByForwardMatch(projTileCodes, coverageId, productKey, zoom, bandNames)("v1")

  /**
   * 读取优化的正向方法
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByForwardMatchV2(projTileCodes: Array[(Int, Int)],
                                       coverageId: String,
                                       productKey: String,
                                       zoom: Int,
                                       bandNames: Set[String]): MultibandTileLayerRDD[SpatialKey] =
    generateVisTilesByForwardMatch(projTileCodes, coverageId, productKey, zoom, bandNames)("v2")

  /**
   * 基于反向瓦片匹配，每个COG瓦片只读一次，生成可视瓦片需要Shuffle
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @param version       读取优化版本标识
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByInvertedMatch(projTileCodes: Array[(Int, Int)],
                                              coverageId: String,
                                              productKey: String,
                                              zoom: Int,
                                              bandNames: Set[String])
                                             (version: String): MultibandTileLayerRDD[SpatialKey] = {
    val cogMetaList = cogMetaQuery(coverageId, productKey, bandNames)
    val crs = cogMetaList.head.crs
    val queryGeometry = cogMetaList.head.geom
    val obsConfTuple = OBSConf.toTuple

    def invertedGenerateFunc(): Iterator[RawTile] => Iterator[RawTile] = par =>
      if (version == "v1") invertedReadV1(par, obsConfTuple)
      else invertedReadV2(par, obsConfTuple)

    val rawTileRdd = invertedMatch(obsConfTuple, cogMetaList, projTileCodes, queryGeometry, zoom)
      .mapPartitions(invertedGenerateFunc())
    //    println("Loading data Time: " + (System.currentTimeMillis() - start))
    val coverage = reprojectByShuffle(rawTileRdd, zoom, crs)
    MultibandTileLayerRDD(coverage, collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 反向瓦片匹配
   *
   * @param obsConfTuple  对象存储配置元组
   * @param cogMetaList   COG元数据列表
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param queryGeometry COG影像范围
   * @param zoom          当前可视层级
   * @return COG瓦片元数据RDD，其中RawTile包含了匹配关系
   */
  private def invertedMatch(obsConfTuple: (String, String, String, String, Int),
                            cogMetaList: ListBuffer[CoverageMetadata],
                            projTileCodes: Array[(Int, Int)],
                            queryGeometry: Geometry,
                            zoom: Int): RDD[RawTile] = {
    val tileMetadata = sc.makeRDD(cogMetaList)
    val time1 = System.currentTimeMillis
    val obsConfTuple = OBSConf.toTuple
    val tileRddFlat = tileMetadata
      .mapPartitions(par => {
        val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
        bucketName = obsConfTuple._4
        par.map(t => { // 合并所有的元数据（追加了范围）
          val rawTiles: ArrayBuffer[RawTile] = cogTileQueryForM2(obs, zoom, t, projTileCodes, queryGeometry)
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
      }).flatMap(t => t).persist
    val tileNum = tileRddFlat.count.toInt
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    println("tileNum = " + tileNum)
    if (tileNum <= 0) {
      throw new Exception("There are no tiles within the visible range!")
    }

    val tileRddRePar = tileRddFlat.repartition(math.min(tileNum, partitionNum))
    tileRddFlat.unpersist()
  }

  /**
   * 反向方法中的读取流程，读取未优化版
   *
   * @param par          分区内的数据
   * @param obsConfTuple 对象存储配置元组
   * @return 带有读取后数据的RDD[RawTile]
   */
  def invertedReadV1(par: Iterator[RawTile], obsConfTuple: OBSConfTuple): Iterator[RawTile] = {
    val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
    val uuid = UUID.randomUUID.toString
    par.map(t => {
      bucketName = obsConfTuple._4
      //        val time2 = System.currentTimeMillis
      //        println(s"rs，${t.path}于${t.offset}处瓦片开始读取，起始时间为$time2，分区：$uuid")
      getCOGTileByRawTile(obs, t)
      //        val time3 = System.currentTimeMillis
      //        println(s"re，${t.path}于${t.offset}处瓦片读取结束，但未反序列化，结束时间为$time3，耗时：${time3 - time2}ms，分区：$uuid")
      t
    })
  }

  /**
   * 反向方法中的读取流程，读取优化版
   *
   * @param par          分区内的数据
   * @param obsConfTuple 对象存储配置元组
   * @return 带有读取后数据的RDD[RawTile]
   */
  def invertedReadV2(par: Iterator[RawTile], obsConfTuple: OBSConfTuple): Iterator[RawTile] = {
    val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
    val uuid = UUID.randomUUID.toString
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
        bucketName = obsConfTuple._4
        val startPos = ab.head.offset
        val byteCount = ab.last.offset - startPos + TILE_BYTE_COUNT
        //          val time1 = System.currentTimeMillis
        //          println(s"rs，${ab.head.path}于${startPos}处瓦片集合开始读取，共读取${byteCount}字节，起始时间为$time1，分区：$uuid")
        getCOGTileByRawTileArray(obs, ab, path, startPos, byteCount)
        //          val time2 = System.currentTimeMillis
        //          println(s"re，${ab.head.path}于${startPos}处瓦片集合读取结束，结束时间为$time2，耗时：${time2 - time1}ms，分区：$uuid")
        ab
      }
      .iterator
  }

  /**
   * 读取未优化的反向方法
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByInvertedMatchV1(projTileCodes: Array[(Int, Int)],
                                        coverageId: String,
                                        productKey: String,
                                        zoom: Int,
                                        bandNames: Set[String]): MultibandTileLayerRDD[SpatialKey] =
    generateVisTilesByInvertedMatch(projTileCodes, coverageId, productKey, zoom, bandNames)("v1")

  /**
   * 读取优化的反向方法
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @return 生成的投影瓦片集合
   */
  def generateVisTilesByInvertedMatchV2(projTileCodes: Array[(Int, Int)],
                                        coverageId: String,
                                        productKey: String,
                                        zoom: Int,
                                        bandNames: Set[String]): MultibandTileLayerRDD[SpatialKey] =
    generateVisTilesByInvertedMatch(projTileCodes, coverageId, productKey, zoom, bandNames)("v2")

  /**
   * 每个COG瓦片只读一次，参与后续计算
   *
   * @param projTileCodes 包含所有需要生成的投影瓦片的编号的数组
   * @param coverageId    影像ID
   * @param productKey    产品号
   * @param zoom          前端地图层级
   * @param bandNames     要使用的波段名
   * @return 生成的投影瓦片集合
   */
  def readRawTiles(projTileCodes: Array[(Int, Int)],
                   coverageId: String,
                   productKey: String,
                   zoom: Int,
                   bandNames: Set[String]): RawMultibandTilesWithMeta = {
    val metaList = cogMetaQuery(coverageId, productKey, bandNames)
    val crs = metaList.head.crs
    val queryGeometry = metaList.head.geom
    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)

    val time1 = System.currentTimeMillis
    val obsConfTuple = OBSConf.toTuple
    val tileRddFlat = tileMetadata
      .mapPartitions(par => {
        val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
        bucketName = obsConfTuple._4
        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
          //          val time1: Long = System.currentTimeMillis()
          val rawTiles = cogTileQueryForM2(obs, zoom, t, projTileCodes, queryGeometry)
          //          val time2: Long = System.currentTimeMillis()
          //          println("Get Tiles Meta Time is " + (time2 - time1))
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
        result
      }).flatMap(t => t).collect
    println(s"头部解析耗时：${System.currentTimeMillis - time1}")
    var (xMin, xMax, yMin, yMax) = (Int.MaxValue, Int.MinValue, Int.MaxValue, Int.MinValue)
    tileRddFlat.foreach(rt => {
      xMin = math.min(xMin, rt.spatialKey.col)
      xMax = math.max(xMax, rt.spatialKey.col)
      yMin = math.min(yMin, rt.spatialKey.row)
      yMax = math.max(yMax, rt.spatialKey.row)
    })
    val tileNum: Int = tileRddFlat.length
    println("tileNum = " + tileNum)
    if (tileNum <= 0) {
      throw new Exception("There are no tiles within the visible range!")
    }
    val tileRddRePar = sc.parallelize(tileRddFlat, math.min(tileNum, partitionNum))
    val rawTileRdd = tileRddRePar
      .mapPartitions(par => {
        val obs = getMinioClient(obsConfTuple._1, obsConfTuple._2, obsConfTuple._3)
        val uuid = UUID.randomUUID.toString
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
            bucketName = obsConfTuple._4
            val startPos = ab.head.offset
            val byteCount = ab.last.offset - startPos + TILE_BYTE_COUNT
            //            val time1 = System.currentTimeMillis
            //            println(s"rs，${ab.head.path}于${startPos}处瓦片集合开始读取，共读取${byteCount}字节，起始时间为$time1，分区：$uuid")
            getCOGTileByRawTileArray(obs, ab, path, startPos, byteCount)
            //            val time2 = System.currentTimeMillis
            //            println(s"re，${ab.head.path}于${startPos}处瓦片集合读取结束，结束时间为$time2，耗时：${time2 - time1}ms，分区：$uuid")
            ab
          }
          .iterator
      })
      .groupBy(_.spatialKey)
      .map { case (key, rawTiles) =>
        val rawTileHead = rawTiles.head
        val tileArraysBuffer = ArrayBuffer.empty[(Int, Array[Int])]
        rawTiles.foreach(rawTile => tileArraysBuffer.append((rawTile.measurementRank, rawTile.tile.toArray)))
        val tileArrays = tileArraysBuffer.sortBy(_._1).map(_._2).toVector
        (key, RawMultibandTile(tileArrays, rawTileHead.extent, rawTileHead.crs, rawTileHead.projCodes))
      }
    (rawTileRdd, (xMin, xMax, yMin, yMax), crs)
  }

  /**
   * 存储可视瓦片
   *
   * @param visTiles 可视瓦片集合
   * @param jobId    任务ID
   * @param zoom     当前层级
   */
  def writeVisTiles(visTiles: MultibandTileLayerRDD[SpatialKey],
                    jobId: String,
                    zoom: Int): Unit = {
    val start = System.currentTimeMillis
    val store = FileAttributeStore(storePath)
    val writer = FileLayerWriter(store)
    Pyramid.upLevels(visTiles, LAYOUT_SCHEME, zoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId(jobId, z)
      if (store.layerExists(layerId)) {
        try {
          writer.overwrite(layerId, rdd)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      } else {
        writer.write(layerId, rdd, ZCurveKeyIndexMethod)
      }
    }
    println("瓦片写入耗时：" + (System.currentTimeMillis - start))
  }

  /**
   * 存储可视瓦片
   *
   * @param visTiles 可视瓦片集合
   * @param jobId    任务ID
   * @param zoom     当前层级
   * @param format   格式
   */
  def writeVisTilesV2(visTiles: MultibandTileLayerRDD[SpatialKey],
                      jobId: String,
                      zoom: Int,
                      format: String): Unit = {
    val start = System.currentTimeMillis
    val store = s"$storePath/$jobId/$zoom/$format"
    // 检查store是否存在，不存在则创建
    val file = new File(store)
    if (!file.exists) file.mkdirs
    format match {
      case "png" => visTiles.foreach { case (key, multibandTile) =>
        multibandTile.renderPng.write(s"$store/${key.col}-${key.row}.$format")
      }
      case "jpg" => visTiles.foreach { case (key, multibandTile) =>
        multibandTile.renderJpg.write(s"$store/${key.col}-${key.row}.$format")
      }
      case _ => println("格式错误")
    }
    println("瓦片写入耗时：" + (System.currentTimeMillis - start))
  }

  /**
   * 存储单波段可视瓦片
   *
   * @param visTiles 可视瓦片集合
   * @param jobId    任务ID
   * @param zoom     当前层级
   * @param format   格式
   */
  def writeVisTilesSingleBand(visTiles: MultibandTileLayerRDD[SpatialKey],
                              jobId: String,
                              zoom: Int,
                              format: String): Unit = {
    val start = System.currentTimeMillis
    val store = s"$storePath/$jobId/$zoom/$format"
    // 检查store是否存在，不存在则创建
    val file = new File(store)
    if (!file.exists) file.mkdirs
    format match {
      case "png" => visTiles.foreach { case (key, multibandTile) =>
        multibandTile.bands(0).renderPng.write(s"$store/${key.col}-${key.row}.$format")
      }
      case "jpg" => visTiles.foreach { case (key, multibandTile) =>
        multibandTile.bands(0).renderJpg.write(s"$store/${key.col}-${key.row}.$format")
      }
      case _ => println("格式错误")
    }
    println("瓦片写入耗时：" + (System.currentTimeMillis - start))
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
    TileLayerMetadata(IntConstantNoDataCellType, LAYOUTS(zoom),
      Extent(leftBottom.xmin, leftBottom.ymin, rightTop.xmax, rightTop.ymax),
      WebMercator, Bounds(SpatialKey(minXCode, minYCode), SpatialKey(maxXCode, maxYCode)))
  }

  /**
   * 生成单个瓦片的重投影
   *
   * @param srcTileArray 原始瓦片集合
   * @param extent       目标瓦片的地理坐标范围
   * @param crs          原始坐标信息
   * @return 重投影后的单个瓦片
   */
  private def reproject(srcTileArray: ArrayBuffer[(Tile, Extent)],
                        extent: Extent,
                        crs: CRS): Raster[Tile] = {
    val (xMin, yMin) = GEO_TO_PROJ(extent.xmin, extent.ymin)
    val (xMax, yMax) = GEO_TO_PROJ(extent.xmax, extent.ymax)
    val destExtent = Extent(xMin, yMin, xMax, yMax)
    val destRaster: Raster[Tile] = Raster(ArrayTile.empty(IntConstantNoDataCellType, 256, 256), destExtent)
    val rrp = implicitly[RasterRegionReproject[Tile]]
    srcTileArray.foreach { case (tile, innerExtent) =>
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
        raster, crs, WebMercator, destRaster,
        ProjectedExtent(innerExtent, crs).reprojectAsPolygon(WebMercator, 0.05),
        ResampleMethod.DEFAULT, 0.05)
    }
    destRaster
  }

  /**
   * 采用Shuffle方式重投影
   *
   * @param rawTileRdd 原始瓦片集合
   * @param zoom       当前层级
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

    def createCombiner(tup: (Raster[Tile], RasterExtent, Polygon)): Tile = {
      val (raster, destRE, destRegion) = tup
      rrp.regionReproject(raster, crs, WebMercator, destRE, destRegion, ResampleMethod.DEFAULT, 0.05).tile
    }

    def mergeValues(reprojectedTile: Tile, toReproject: (Raster[Tile], RasterExtent, Polygon)): Tile = {
      val (raster, destRE, destRegion) = toReproject
      val destRaster = Raster(reprojectedTile, destRE.extent)
      rrp.regionReprojectMutable(raster, crs, WebMercator, destRaster, destRegion, ResampleMethod.DEFAULT, 0.05).tile
    }

    def mergeCombiners(reproj1: Tile, reproj2: Tile): Tile = reproj1.merge(reproj2)

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

  /**
   * 采用Shuffle方式重投影
   *
   * @param rawMultibandTilesWithMeta 原始瓦片集合
   * @param projTileCodes             所有的投影瓦片号
   * @param zoom                      当前层级
   * @return 重投影后的瓦片集合
   */
  def rmtReprojectByShuffle(rawMultibandTilesWithMeta: RawMultibandTilesWithMeta,
                            projTileCodes: Array[(Int, Int)],
                            zoom: Int): MultibandTileLayerRDD[SpatialKey] = {
    val (rawMultibandTileRdd, _, crs) = rawMultibandTilesWithMeta
    val stagedRdd = rawMultibandTileRdd.flatMap { case (_, rawMultibandTile) =>
      val (tile, innerExtent) = (MultibandTile(rawMultibandTile.tileArrays.map(tileArray =>
        IntArrayTile(tileArray, TILE_SIZE, TILE_SIZE, IntConstantNoDataCellType))), rawMultibandTile.extent)
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
      val destRegion = ProjectedExtent(innerExtent, rawMultibandTile.crs).reprojectAsPolygon(WebMercator, 0.05)
      rawMultibandTile.projCodes.map(code => {
        val destRE = RasterExtent(code.extent(LAYOUTS(zoom)), 256, 256)
        (code, (raster, destRE, destRegion))
      })
    }

    val rrp = implicitly[RasterRegionReproject[MultibandTile]]

    def createCombiner(tup: (Raster[MultibandTile], RasterExtent, Polygon)): MultibandTile = {
      val (raster, destRE, destRegion) = tup
      rrp.regionReproject(raster, crs, WebMercator, destRE, destRegion, ResampleMethod.DEFAULT, 0.05).tile
    }

    def mergeValues(reprojectedTile: MultibandTile,
                    toReproject: (Raster[MultibandTile], RasterExtent, Polygon)): MultibandTile = {
      val (raster, destRE, destRegion) = toReproject
      val destRaster = Raster(reprojectedTile, destRE.extent)
      rrp.regionReprojectMutable(raster, crs, WebMercator, destRaster, destRegion, ResampleMethod.DEFAULT, 0.05).tile
    }

    def mergeCombiners(reproj1: MultibandTile, reproj2: MultibandTile): MultibandTile = reproj1.merge(reproj2)

    MultibandTileLayerRDD(stagedRdd.combineByKey(createCombiner, mergeValues, mergeCombiners),
      collectMetadataFromProjTileCodes(projTileCodes, zoom))
  }

  /**
   * 在同一个key中计算，不同key之间相互不影响
   *
   * @param coverages 输入的瓦片集合序列
   * @param operator  使用的算子
   * @return 计算结果
   */
  private def computeInKey(coverages: Seq[MultibandTileLayerRDD[SpatialKey]],
                           operator: Seq[MultibandTile] => MultibandTile): MultibandTileLayerRDD[SpatialKey] = {
    val union = coverages.reduce(_ merge _)
    val coverage = union
      .groupByKey
      .map { case (key, multibandTiles) => (key, operator(multibandTiles.toSeq)) }
    val metadata = union.metadata
    MultibandTileLayerRDD(coverage, metadata)
  }

  def normalizedDifference(coverages: Seq[MultibandTileLayerRDD[SpatialKey]]): MultibandTileLayerRDD[SpatialKey] =
    computeInKey(coverages, normalizedDifferenceOp)

  /**
   * 在不同key中计算，不同key之间相互影响
   *
   * @param coverages 输入的瓦片集合序列
   * @param operators 使用的算子
   * @param radius    邻域半径
   * @return 计算结果
   */
  private def computeOutKeyV1[MR](coverages: Seq[MultibandTileLayerRDD[SpatialKey]],
                                  operators: ComputeOutKeyOperators[MR],
                                  radius: Int)
                                 (implicit tag: ClassTag[MR]): MultibandTileLayerRDD[SpatialKey] = {
    def locationProcess(location: (Int, Int), radius: Int):
    ((Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int) => (Int, Int, Int, Int), Boolean) =
      location match {
        case (0, 0) => (null, null, null, true)
        case (0, -1) => (
          (0, radius - 1, 0, TILE_MAX_INDEX), (TILE_SIZE - radius, TILE_MAX_INDEX, 0, TILE_MAX_INDEX), { (tr, tc) => (tr, TILE_MAX_INDEX, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }, false
        ) // 上
        case (0, 1) => (
          (TILE_SIZE - radius, TILE_MAX_INDEX, 0, TILE_MAX_INDEX), (0, radius - 1, 0, TILE_MAX_INDEX), { (tr, tc) => (0, tr, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }, false
        ) // 下
        case (-1, 0) => (
          (0, TILE_MAX_INDEX, 0, radius - 1), (0, TILE_MAX_INDEX, TILE_SIZE - radius, TILE_MAX_INDEX), { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), tc, TILE_MAX_INDEX) }, false
        ) // 左
        case (1, 0) => (
          (0, TILE_MAX_INDEX, TILE_SIZE - radius, TILE_MAX_INDEX), (0, TILE_MAX_INDEX, 0, radius - 1), { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), 0, tc) }, false
        ) // 右
        case (-1, -1) => (
          (0, radius - 1, 0, radius - 1), (TILE_SIZE - radius, TILE_MAX_INDEX, TILE_SIZE - radius, TILE_MAX_INDEX), { (tr, tc) => (tr, TILE_MAX_INDEX, tc, TILE_MAX_INDEX) }, false
        ) // 左上
        case (1, -1) => (
          (0, radius - 1, TILE_SIZE - radius, TILE_MAX_INDEX), (TILE_SIZE - radius, TILE_MAX_INDEX, 0, radius - 1), { (tr, tc) => (tr, TILE_MAX_INDEX, 0, tc) }, false
        ) // 右上
        case (-1, 1) => (
          (TILE_SIZE - radius, TILE_MAX_INDEX, 0, radius - 1), (0, radius - 1, TILE_SIZE - radius, TILE_MAX_INDEX), { (tr, tc) => (0, tr, tc, TILE_MAX_INDEX) }, false
        ) // 左下
        case (1, 1) => (
          (TILE_SIZE - radius, TILE_MAX_INDEX, TILE_SIZE - radius, TILE_MAX_INDEX), (0, radius - 1, 0, radius - 1), { (tr, tc) => (0, tr, 0, tc) }, false
        ) // 右下
      }

    def createCombiner(tileWithLocation: ((Int, Int), Vector[Array[Int]])): Vector[Array[MR]] = {
      val (location, tileArrays) = tileWithLocation
      val (mrRange, tileRange, rangeChange, flag) = locationProcess(location, radius)
      if (flag) {
        tileArrays.map(tileArray => {
          val mrArray = new Array[MR](TILE_CELL_COUNT)
          operators.inKeyOp(tileArray, mrArray, radius)
          mrArray
        })
      } else {
        tileArrays.map(tileArray => {
          val mrArray = Array.fill(TILE_CELL_COUNT)(operators.createMr)
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArray, mrArray, rangeChange(tileRow, tileCol), TILE_SIZE, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          mrArray
        })
      }
    }

    def mergeValues(computed: Vector[Array[MR]],
                    toCompute: ((Int, Int), Vector[Array[Int]])): Vector[Array[MR]] = {
      val (location, tileArrays) = toCompute
      val (mrRange, tileRange, rangeChange, flag) = locationProcess(location, radius)
      var bandNum = 0
      if (flag) {
        while (bandNum < tileArrays.size) {
          operators.inKeyOp(tileArrays(bandNum), computed(bandNum), radius)
          bandNum += 1
        }
      } else {
        while (bandNum < tileArrays.size) {
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArrays(bandNum), computed(bandNum),
                rangeChange(tileRow, tileCol), TILE_SIZE, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          bandNum += 1
        }
      }
      computed
    }

    def mergeCombiners(computed1: Vector[Array[MR]],
                       computed2: Vector[Array[MR]]): Vector[Array[MR]] = {
      var bandNum = 0
      while (bandNum < computed1.size) {
        operators.combKeyOp(computed1(bandNum), computed2(bandNum))
        bandNum += 1
      }
      computed1
    }

    val union = coverages.reduce(_ merge _)
    val metadata = union.metadata
    val KeyBounds(SpatialKey(xMin, yMin), SpatialKey(xMax, yMax)) = metadata.bounds.get
    val coverage = union
      .groupByKey
      .flatMap { case (key, multibandTiles) =>
        val SpatialKey(x, y) = key
        multibandTiles.zipWithIndex.flatMap { case (multibandTile, measurementRank) =>
          val tileArrays = multibandTile.bands.map(_.toArray)
          val ab = ArrayBuffer.empty[(SpaceBandKey, ((Int, Int), Vector[Array[Int]]))]
          ab.append((SpaceBandKey(SpatialKey(x, y), measurementRank), ((0, 0), tileArrays)))
          if (y < yMax) {
            ab.append((SpaceBandKey(SpatialKey(x, y + 1), measurementRank), ((0, -1), tileArrays))) // 上
            if (x < xMax) ab.append((SpaceBandKey(SpatialKey(x + 1, y + 1), measurementRank), ((-1, -1), tileArrays))) // 左上
            if (x > xMin) ab.append((SpaceBandKey(SpatialKey(x - 1, y + 1), measurementRank), ((1, -1), tileArrays))) // 右上
          }
          if (y > yMin) {
            ab.append((SpaceBandKey(SpatialKey(x, y - 1), measurementRank), ((0, 1), tileArrays))) // 下
            if (x < xMax) ab.append((SpaceBandKey(SpatialKey(x + 1, y - 1), measurementRank), ((-1, 1), tileArrays))) // 左下
            if (x > xMin) ab.append((SpaceBandKey(SpatialKey(x - 1, y - 1), measurementRank), ((1, 1), tileArrays))) // 右下
          }
          if (x < xMax) ab.append((SpaceBandKey(SpatialKey(x + 1, y), measurementRank), ((-1, 0), tileArrays))) // 左
          if (x > xMin) ab.append((SpaceBandKey(SpatialKey(x - 1, y), measurementRank), ((1, 0), tileArrays))) // 右
          ab
        }
      }
      .combineByKey(createCombiner, mergeValues, mergeCombiners)
      .map { case (key, multibandTileArrays) =>
        (key.spatialKey, MultibandTile(multibandTileArrays
          .map(tileArray => IntArrayTile(
            tileArray.map(operators.finalMapOp), TILE_SIZE, TILE_SIZE, IntConstantNoDataCellType))))
      }
      .groupByKey
      .map { case (key, multibandTiles) => (key, operators.sameKeyOp(multibandTiles.toSeq)) }
    MultibandTileLayerRDD(coverage, metadata)
  }

  /**
   * 在不同key中计算，不同key之间相互影响，手动复制数据
   *
   * @param coverages 输入的瓦片集合序列
   * @param operators 使用的算子
   * @param radius    邻域半径
   * @return 计算结果
   */
  private def computeOutKeyV2[MR](coverages: Seq[MultibandTileLayerRDD[SpatialKey]],
                                  operators: ComputeOutKeyOperators[MR],
                                  radius: Int)
                                 (implicit tag: ClassTag[MR]): MultibandTileLayerRDD[SpatialKey] = {
    val (ltIndex, rbIndex) = (radius - 1, TILE_SIZE - radius)

    def locationProcess(location: (Int, Int), radius: Int):
    ((Int, Int, Int, Int), (Int, Int, Int, Int), Int, Boolean, (Int, Int) => (Int, Int, Int, Int)) =
      location match {
        case (0, 0) => (null, null, 0, true, null)
        case (0, -1) => (
          (0, ltIndex, 0, TILE_MAX_INDEX), (0, ltIndex, 0, TILE_MAX_INDEX), TILE_SIZE, false, { (tr, tc) => (tr, ltIndex, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }
        ) // 上
        case (0, 1) => (
          (rbIndex, TILE_MAX_INDEX, 0, TILE_MAX_INDEX), (0, ltIndex, 0, TILE_MAX_INDEX), TILE_SIZE, false, { (tr, tc) => (0, tr, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }
        ) // 下
        case (-1, 0) => (
          (0, TILE_MAX_INDEX, 0, ltIndex), (0, TILE_MAX_INDEX, 0, ltIndex), radius, false, { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), tc, ltIndex) }
        ) // 左
        case (1, 0) => (
          (0, TILE_MAX_INDEX, rbIndex, TILE_MAX_INDEX), (0, TILE_MAX_INDEX, 0, ltIndex), radius, false, { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), 0, tc) }
        ) // 右
        case (-1, -1) => (
          (0, ltIndex, 0, ltIndex), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (tr, ltIndex, tc, ltIndex) }
        ) // 左上
        case (1, -1) => (
          (0, ltIndex, rbIndex, TILE_MAX_INDEX), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (tr, ltIndex, 0, tc) }
        ) // 右上
        case (-1, 1) => (
          (rbIndex, TILE_MAX_INDEX, 0, radius - 1), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (0, tr, tc, ltIndex) }
        ) // 左下
        case (1, 1) => (
          (rbIndex, TILE_MAX_INDEX, rbIndex, TILE_MAX_INDEX), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (0, tr, 0, tc) }
        ) // 右下
      }

    def createCombiner(tileWithLocation: ((Int, Int), Vector[Array[Int]])): Vector[Array[MR]] = {
      val (location, tileArrays) = tileWithLocation
      val (mrRange, tileRange, tileMaxCol, flag, rangeChange) = locationProcess(location, radius)
      if (flag) {
        tileArrays.map(tileArray => {
          val mrArray = new Array[MR](TILE_CELL_COUNT)
          operators.inKeyOp(tileArray, mrArray, radius)
          mrArray
        })
      } else {
        tileArrays.map(tileArray => {
          val mrArray = Array.fill(TILE_CELL_COUNT)(operators.createMr)
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArray, mrArray, rangeChange(tileRow, tileCol), tileMaxCol, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          mrArray
        })
      }
    }

    def mergeValues(computed: Vector[Array[MR]],
                    toCompute: ((Int, Int), Vector[Array[Int]])): Vector[Array[MR]] = {
      val (location, tileArrays) = toCompute
      val (mrRange, tileRange, tileMaxCol, flag, rangeChange) = locationProcess(location, radius)
      var bandNum = 0
      if (flag) {
        while (bandNum < tileArrays.size) {
          operators.inKeyOp(tileArrays(bandNum), computed(bandNum), radius)
          bandNum += 1
        }
      } else {
        while (bandNum < tileArrays.size) {
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArrays(bandNum), computed(bandNum),
                rangeChange(tileRow, tileCol), tileMaxCol, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          bandNum += 1
        }
      }
      computed
    }

    def mergeCombiners(computed1: Vector[Array[MR]],
                       computed2: Vector[Array[MR]]): Vector[Array[MR]] = {
      var bandNum = 0
      while (bandNum < computed1.size) {
        operators.combKeyOp(computed1(bandNum), computed2(bandNum))
        bandNum += 1
      }
      computed1
    }

    val union = coverages.reduce(_ merge _)
    val metadata = union.metadata
    val KeyBounds(SpatialKey(xMin, yMin), SpatialKey(xMax, yMax)) = metadata.bounds.get

    def newArray(srcArray: Array[Int],
                 range: (Int, Int),
                 srcLeftTopIndex: (Int, Int)): Array[Int] = {
      val newArray = new Array[Int](range._1 * range._2)
      var (newRow, srcRow) = (0, srcLeftTopIndex._1)
      while (newRow < range._1) {
        val (newRowNum, srcRowNum) = (newRow * range._2, srcRow * TILE_SIZE)
        var (newCol, srcCol) = (0, srcLeftTopIndex._2)
        while (newCol < range._2) {
          newArray(newRowNum + newCol) = srcArray(srcRowNum + srcCol)
          newCol += 1
          srcCol += 1
        }
        newRow += 1
        srcRow += 1
      }
      newArray
    }

    val coverage = union
      .groupByKey
      .flatMap { case (key, multibandTiles) =>
        val SpatialKey(x, y) = key
        multibandTiles.zipWithIndex.flatMap { case (multibandTile, measurementRank) =>
          val tileArrays = multibandTile.bands.map(_.toArray)
          val ab = ArrayBuffer.empty[(SpaceBandKey, ((Int, Int), Vector[Array[Int]]))]
          ab.append((SpaceBandKey(SpatialKey(x, y), measurementRank), ((0, 0), tileArrays)))
          if (y < yMax) {
            ab.append((SpaceBandKey(SpatialKey(x, y + 1), measurementRank),
              ((0, -1), tileArrays.map(tileArray => newArray(tileArray, (radius, TILE_SIZE), (0, 0)))))) // 上
            if (x < xMax) ab.append((SpaceBandKey(SpatialKey(x + 1, y + 1), measurementRank),
              ((-1, -1), tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (0, 0)))))) // 左上
            if (x > xMin) ab.append((SpaceBandKey(SpatialKey(x - 1, y + 1), measurementRank),
              ((1, -1), tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (0, rbIndex)))))) // 右上
          }
          if (y > yMin) {
            ab.append((SpaceBandKey(SpatialKey(x, y - 1), measurementRank),
              ((0, 1), tileArrays.map(tileArray => newArray(tileArray, (radius, TILE_SIZE), (rbIndex, 0)))))) // 下
            if (x < xMax) ab.append((SpaceBandKey(SpatialKey(x + 1, y - 1), measurementRank),
              ((-1, 1), tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (rbIndex, 0)))))) // 左下
            if (x > xMin) ab.append((SpaceBandKey(SpatialKey(x - 1, y - 1), measurementRank),
              ((1, 1), tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (rbIndex, rbIndex)))))) // 右下
          }
          if (x < xMax) ab.append((SpaceBandKey(SpatialKey(x + 1, y), measurementRank),
            ((-1, 0), tileArrays.map(tileArray => newArray(tileArray, (TILE_SIZE, radius), (0, 0)))))) // 左
          if (x > xMin) ab.append((SpaceBandKey(SpatialKey(x - 1, y), measurementRank),
            ((1, 0), tileArrays.map(tileArray => newArray(tileArray, (TILE_SIZE, radius), (0, rbIndex)))))) // 右
          ab
        }
      }
      .combineByKey(createCombiner, mergeValues, mergeCombiners)
      .map { case (key, multibandTileArrays) =>
        (key.spatialKey, MultibandTile(multibandTileArrays
          .map(tileArray => IntArrayTile(
            tileArray.map(operators.finalMapOp), TILE_SIZE, TILE_SIZE, IntConstantNoDataCellType))))
      }
      .groupByKey
      .map { case (key, multibandTiles) => (key, operators.sameKeyOp(multibandTiles.toSeq)) }
    MultibandTileLayerRDD(coverage, metadata)
  }

  /**
   * 在不同key中计算，不同key之间相互影响
   *
   * @param rawMultibandTilesWithMeta 带元数据的输入的瓦片集合
   * @param operators                 使用的算子
   * @param radius                    邻域半径
   * @return 计算结果
   */
  private def computeOutRawKeyV1[MR](rawMultibandTilesWithMeta: RawMultibandTilesWithMeta,
                                     operators: ComputeOutKeyOperators[MR],
                                     radius: Int)
                                    (implicit tag: ClassTag[MR]): RawMultibandTilesWithMeta = {
    val (coverage, rawKeyRange, crs) = rawMultibandTilesWithMeta
    val (ltIndex, rbIndex) = (radius - 1, TILE_SIZE - radius)

    def locationProcess(location: (Int, Int), radius: Int):
    ((Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int) => (Int, Int, Int, Int), Boolean) =
      location match {
        case (0, 0) => (null, null, null, true)
        case (0, -1) => (
          (0, ltIndex, 0, TILE_MAX_INDEX), (rbIndex, TILE_MAX_INDEX, 0, TILE_MAX_INDEX), { (tr, tc) => (tr, TILE_MAX_INDEX, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }, false
        ) // 上
        case (0, 1) => (
          (rbIndex, TILE_MAX_INDEX, 0, TILE_MAX_INDEX), (0, ltIndex, 0, TILE_MAX_INDEX), { (tr, tc) => (0, tr, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }, false
        ) // 下
        case (-1, 0) => (
          (0, TILE_MAX_INDEX, 0, ltIndex), (0, TILE_MAX_INDEX, TILE_SIZE - radius, TILE_MAX_INDEX), { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), tc, TILE_MAX_INDEX) }, false
        ) // 左
        case (1, 0) => (
          (0, TILE_MAX_INDEX, TILE_SIZE - radius, TILE_MAX_INDEX), (0, TILE_MAX_INDEX, 0, ltIndex), { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), 0, tc) }, false
        ) // 右
        case (-1, -1) => (
          (0, ltIndex, 0, ltIndex), (rbIndex, TILE_MAX_INDEX, rbIndex, TILE_MAX_INDEX), { (tr, tc) => (tr, TILE_MAX_INDEX, tc, TILE_MAX_INDEX) }, false
        ) // 左上
        case (1, -1) => (
          (0, ltIndex, rbIndex, TILE_MAX_INDEX), (rbIndex, TILE_MAX_INDEX, 0, ltIndex), { (tr, tc) => (tr, TILE_MAX_INDEX, 0, tc) }, false
        ) // 右上
        case (-1, 1) => (
          (rbIndex, TILE_MAX_INDEX, 0, ltIndex), (0, ltIndex, rbIndex, TILE_MAX_INDEX), { (tr, tc) => (0, tr, tc, TILE_MAX_INDEX) }, false
        ) // 左下
        case (1, 1) => (
          (rbIndex, TILE_MAX_INDEX, rbIndex, TILE_MAX_INDEX), (0, ltIndex, 0, ltIndex), { (tr, tc) => (0, tr, 0, tc) }, false
        ) // 右下
      }

    def createCombiner(tileWithLocation: ((Int, Int), RawMultibandTile)): RawMultibandTileInCompute[MR] = {
      val (location, rawMultibandTile) = tileWithLocation
      val tileArrays = rawMultibandTile.tileArrays
      val (mrRange, tileRange, rangeChange, flag) = locationProcess(location, radius)
      val mrArrays = if (flag) {
        tileArrays.map(tileArray => {
          val mrArray = new Array[MR](TILE_CELL_COUNT)
          operators.inKeyOp(tileArray, mrArray, radius)
          mrArray
        })
      } else {
        tileArrays.map(tileArray => {
          val mrArray = Array.fill(TILE_CELL_COUNT)(operators.createMr)
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArray, mrArray, rangeChange(tileRow, tileCol), TILE_SIZE, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          mrArray
        })
      }
      RawMultibandTileInCompute(mrArrays, rawMultibandTile.extent, rawMultibandTile.crs, rawMultibandTile.projCodes)
    }

    def mergeValues(computed: RawMultibandTileInCompute[MR],
                    toCompute: ((Int, Int), RawMultibandTile)): RawMultibandTileInCompute[MR] = {
      val (location, rawMultibandTile) = toCompute
      val tileArrays = rawMultibandTile.tileArrays
      val (mrRange, tileRange, rangeChange, flag) = locationProcess(location, radius)
      var bandNum = 0
      if (flag) {
        while (bandNum < tileArrays.size) {
          operators.inKeyOp(tileArrays(bandNum), computed.mrArrays(bandNum), radius)
          bandNum += 1
        }
      } else {
        while (bandNum < tileArrays.size) {
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArrays(bandNum), computed.mrArrays(bandNum),
                rangeChange(tileRow, tileCol), TILE_SIZE, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          bandNum += 1
        }
      }
      computed.projCodes = if (computed.projCodes == null) rawMultibandTile.projCodes else computed.projCodes
      computed
    }

    def mergeCombiners(computed1: RawMultibandTileInCompute[MR],
                       computed2: RawMultibandTileInCompute[MR]): RawMultibandTileInCompute[MR] = {
      var bandNum = 0
      while (bandNum < computed1.mrArrays.size) {
        operators.combKeyOp(computed1.mrArrays(bandNum), computed2.mrArrays(bandNum))
        bandNum += 1
      }
      computed1.projCodes = if (computed1.projCodes == null) computed2.projCodes else computed1.projCodes
      computed1
    }

    val (xMin, xMax, yMin, yMax) = rawKeyRange
    val results = coverage
      .flatMap { case (key, rawMultibandTile) =>
        val SpatialKey(x, y) = key
        val tileArrays = rawMultibandTile.tileArrays
        val outRawMultibandTile = RawMultibandTile(tileArrays, rawMultibandTile.extent, rawMultibandTile.crs, null)
        val ab = ArrayBuffer.empty[(SpatialKey, ((Int, Int), RawMultibandTile))]
        ab.append((key, ((0, 0), rawMultibandTile)))
        if (y < yMax) {
          ab.append((SpatialKey(x, y + 1), ((0, -1), outRawMultibandTile))) // 上
          if (x < xMax) ab.append((SpatialKey(x + 1, y + 1), ((-1, -1), outRawMultibandTile))) // 左上
          if (x > xMin) ab.append((SpatialKey(x - 1, y + 1), ((1, -1), outRawMultibandTile))) // 右上
        }
        if (y > yMin) {
          ab.append((SpatialKey(x, y - 1), ((0, 1), outRawMultibandTile))) // 下
          if (x < xMax) ab.append((SpatialKey(x + 1, y - 1), ((-1, 1), outRawMultibandTile))) // 左下
          if (x > xMin) ab.append((SpatialKey(x - 1, y - 1), ((1, 1), outRawMultibandTile))) // 右下
        }
        if (x < xMax) ab.append((SpatialKey(x + 1, y), ((-1, 0), outRawMultibandTile))) // 左
        if (x > xMin) ab.append((SpatialKey(x - 1, y), ((1, 0), outRawMultibandTile))) // 右
        ab
      }
      .combineByKey(createCombiner, mergeValues, mergeCombiners)
      .map { case (key, rawMultibandTileInCompute) =>
        (key, RawMultibandTile(
          rawMultibandTileInCompute.mrArrays.map(mrArray => mrArray.map(operators.finalMapOp)),
          rawMultibandTileInCompute.extent, rawMultibandTileInCompute.crs, rawMultibandTileInCompute.projCodes))
      }
    (results, rawKeyRange, crs)
  }

  /**
   * 在不同key中计算，不同key之间相互影响，手动复制数据
   *
   * @param rawMultibandTilesWithMeta 带元数据的输入的瓦片集合
   * @param operators                 使用的算子
   * @param radius                    邻域半径
   * @return 计算结果
   */
  private def computeOutRawKeyV2[MR](rawMultibandTilesWithMeta: RawMultibandTilesWithMeta,
                                     operators: ComputeOutKeyOperators[MR],
                                     radius: Int)
                                    (implicit tag: ClassTag[MR]): RawMultibandTilesWithMeta = {
    val (coverage, rawKeyRange, crs) = rawMultibandTilesWithMeta
    val (ltIndex, rbIndex) = (radius - 1, TILE_SIZE - radius)

    def locationProcess(location: (Int, Int), radius: Int):
    ((Int, Int, Int, Int), (Int, Int, Int, Int), Int, Boolean, (Int, Int) => (Int, Int, Int, Int)) =
      location match {
        case (0, 0) => (null, null, 0, true, null)
        case (0, -1) => (
          (0, ltIndex, 0, TILE_MAX_INDEX), (0, ltIndex, 0, TILE_MAX_INDEX), TILE_SIZE, false, { (tr, tc) => (tr, ltIndex, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }
        ) // 上
        case (0, 1) => (
          (rbIndex, TILE_MAX_INDEX, 0, TILE_MAX_INDEX), (0, ltIndex, 0, TILE_MAX_INDEX), TILE_SIZE, false, { (tr, tc) => (0, tr, math.max(0, tc - radius), math.min(tc + radius, TILE_MAX_INDEX)) }
        ) // 下
        case (-1, 0) => (
          (0, TILE_MAX_INDEX, 0, ltIndex), (0, TILE_MAX_INDEX, 0, ltIndex), radius, false, { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), tc, ltIndex) }
        ) // 左
        case (1, 0) => (
          (0, TILE_MAX_INDEX, rbIndex, TILE_MAX_INDEX), (0, TILE_MAX_INDEX, 0, ltIndex), radius, false, { (tr, tc) => (math.max(0, tr - radius), math.min(tr + radius, TILE_MAX_INDEX), 0, tc) }
        ) // 右
        case (-1, -1) => (
          (0, ltIndex, 0, ltIndex), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (tr, ltIndex, tc, ltIndex) }
        ) // 左上
        case (1, -1) => (
          (0, ltIndex, rbIndex, TILE_MAX_INDEX), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (tr, ltIndex, 0, tc) }
        ) // 右上
        case (-1, 1) => (
          (rbIndex, TILE_MAX_INDEX, 0, radius - 1), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (0, tr, tc, ltIndex) }
        ) // 左下
        case (1, 1) => (
          (rbIndex, TILE_MAX_INDEX, rbIndex, TILE_MAX_INDEX), (0, ltIndex, 0, ltIndex), radius, false, { (tr, tc) => (0, tr, 0, tc) }
        ) // 右下
      }

    def createCombiner(tileWithLocation: ((Int, Int), RawMultibandTile)): RawMultibandTileInCompute[MR] = {
      val (location, rawMultibandTile) = tileWithLocation
      val tileArrays = rawMultibandTile.tileArrays
      val (mrRange, tileRange, tileMaxCol, flag, rangeChange) = locationProcess(location, radius)
      val mrArrays = if (flag) {
        tileArrays.map(tileArray => {
          val mrArray = new Array[MR](TILE_CELL_COUNT)
          operators.inKeyOp(tileArray, mrArray, radius)
          mrArray
        })
      } else {
        tileArrays.map(tileArray => {
          val mrArray = Array.fill(TILE_CELL_COUNT)(operators.createMr)
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArray, mrArray, rangeChange(tileRow, tileCol), tileMaxCol, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          mrArray
        })
      }
      RawMultibandTileInCompute(mrArrays, rawMultibandTile.extent, rawMultibandTile.crs, rawMultibandTile.projCodes)
    }

    def mergeValues(computed: RawMultibandTileInCompute[MR],
                    toCompute: ((Int, Int), RawMultibandTile)): RawMultibandTileInCompute[MR] = {
      val (location, rawMultibandTile) = toCompute
      val tileArrays = rawMultibandTile.tileArrays
      val (mrRange, tileRange, tileMaxCol, flag, rangeChange) = locationProcess(location, radius)
      var bandNum = 0
      if (flag) {
        while (bandNum < tileArrays.size) {
          operators.inKeyOp(tileArrays(bandNum), computed.mrArrays(bandNum), radius)
          bandNum += 1
        }
      } else {
        while (bandNum < tileArrays.size) {
          var (tileRow, mrRow) = (tileRange._1, mrRange._1)
          while (tileRow <= tileRange._2) {
            val rowNum = mrRow * TILE_SIZE
            var (tileCol, mrCol) = (tileRange._3, mrRange._3)
            while (tileCol <= tileRange._4) {
              val mrsIndex = rowNum + mrCol
              operators.outKeyOp(tileArrays(bandNum), computed.mrArrays(bandNum),
                rangeChange(tileRow, tileCol), tileMaxCol, location, mrsIndex)
              tileCol += 1
              mrCol += 1
            }
            tileRow += 1
            mrRow += 1
          }
          bandNum += 1
        }
      }
      computed.projCodes = if (computed.projCodes == null) rawMultibandTile.projCodes else computed.projCodes
      computed
    }

    def mergeCombiners(computed1: RawMultibandTileInCompute[MR],
                       computed2: RawMultibandTileInCompute[MR]): RawMultibandTileInCompute[MR] = {
      var bandNum = 0
      while (bandNum < computed1.mrArrays.size) {
        operators.combKeyOp(computed1.mrArrays(bandNum), computed2.mrArrays(bandNum))
        bandNum += 1
      }
      computed1.projCodes = if (computed1.projCodes == null) computed2.projCodes else computed1.projCodes
      computed1
    }

    def newArray(srcArray: Array[Int],
                 range: (Int, Int),
                 srcLeftTopIndex: (Int, Int)): Array[Int] = {
      val newArray = new Array[Int](range._1 * range._2)
      var (newRow, srcRow) = (0, srcLeftTopIndex._1)
      while (newRow < range._1) {
        val (newRowNum, srcRowNum) = (newRow * range._2, srcRow * TILE_SIZE)
        var (newCol, srcCol) = (0, srcLeftTopIndex._2)
        while (newCol < range._2) {
          newArray(newRowNum + newCol) = srcArray(srcRowNum + srcCol)
          newCol += 1
          srcCol += 1
        }
        newRow += 1
        srcRow += 1
      }
      newArray
    }

    val (xMin, xMax, yMin, yMax) = rawKeyRange
    val results = coverage
      .flatMap { case (key, rawMultibandTile) =>
        val SpatialKey(x, y) = key
        val tileArrays = rawMultibandTile.tileArrays
        val ab = ArrayBuffer.empty[(SpatialKey, ((Int, Int), RawMultibandTile))]
        ab.append((key, ((0, 0), rawMultibandTile)))
        if (y < yMax) {
          ab.append((SpatialKey(x, y + 1), ((0, -1),
            RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (radius, TILE_SIZE), (0, 0))),
              rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 上
          if (x < xMax) ab.append((SpatialKey(x + 1, y + 1), ((-1, -1),
            RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (0, 0))),
              rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 左上
          if (x > xMin) ab.append((SpatialKey(x - 1, y + 1), ((1, -1),
            RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (0, rbIndex))),
              rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 右上
        }
        if (y > yMin) {
          ab.append((SpatialKey(x, y - 1), ((0, 1),
            RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (radius, TILE_SIZE), (rbIndex, 0))),
              rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 下
          if (x < xMax) ab.append((SpatialKey(x + 1, y - 1), ((-1, 1),
            RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (rbIndex, 0))),
              rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 左下
          if (x > xMin) ab.append((SpatialKey(x - 1, y - 1), ((1, 1),
            RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (radius, radius), (rbIndex, rbIndex))),
              rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 右下
        }
        if (x < xMax) ab.append((SpatialKey(x + 1, y), ((-1, 0),
          RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (TILE_SIZE, radius), (0, 0))),
            rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 左
        if (x > xMin) ab.append((SpatialKey(x - 1, y), ((1, 0),
          RawMultibandTile(tileArrays.map(tileArray => newArray(tileArray, (TILE_SIZE, radius), (0, rbIndex))),
            rawMultibandTile.extent, rawMultibandTile.crs, null)))) // 右
        ab
      }
      .combineByKey(createCombiner, mergeValues, mergeCombiners)
      .map { case (key, rawMultibandTileInCompute) =>
        (key, RawMultibandTile(
          rawMultibandTileInCompute.mrArrays.map(mrArray => mrArray.map(operators.finalMapOp)),
          rawMultibandTileInCompute.extent, rawMultibandTileInCompute.crs, rawMultibandTileInCompute.projCodes))
      }
    (results, rawKeyRange, crs)
  }

  /**
   * 在不同key中计算，不同key之间相互影响
   *
   * @param coverages 输入的瓦片集合序列
   * @param operators 使用的算子
   * @param radius    邻域半径
   * @return 计算结果
   */
  private def computeOutKey[MR](coverages: Seq[MultibandTileLayerRDD[SpatialKey]],
                                operators: ComputeOutKeyOperators[MR],
                                radius: Int)
                               (version: String)
                               (implicit tag: ClassTag[MR]): MultibandTileLayerRDD[SpatialKey] = version match {
    case "v1" => computeOutKeyV1[MR](coverages, operators, radius)
    case "v2" => computeOutKeyV2[MR](coverages, operators, radius)
    case _ => throw new IllegalArgumentException("version must be v1 or v2")
  }

  /**
   * 在不同key中计算，不同key之间相互影响
   *
   * @param rawMultibandTilesWithMeta 带元数据的输入的瓦片集合
   * @param operators                 使用的算子
   * @param radius                    邻域半径
   * @return 计算结果
   */
  private def computeOutRawKey[MR](rawMultibandTilesWithRange: RawMultibandTilesWithMeta,
                                   operators: ComputeOutKeyOperators[MR],
                                   radius: Int)
                                  (version: String)
                                  (implicit tag: ClassTag[MR]): RawMultibandTilesWithMeta = version match {
    case "v1" => computeOutRawKeyV1[MR](rawMultibandTilesWithRange, operators, radius)
    case "v2" => computeOutRawKeyV2[MR](rawMultibandTilesWithRange, operators, radius)
    case _ => throw new IllegalArgumentException("version must be v1 or v2")
  }

  def focalMean(coverages: Seq[MultibandTileLayerRDD[SpatialKey]], radius: Int)
               (version: String = "v1"): MultibandTileLayerRDD[SpatialKey] =
    computeOutKey[(Int, Int)](coverages, FocalMeanOperators(), radius)(version)

  def rawFocalMean(rawMultibandTilesWithRange: RawMultibandTilesWithMeta, radius: Int)
                  (version: String = "v1"): RawMultibandTilesWithMeta =
    computeOutRawKey[(Int, Int)](rawMultibandTilesWithRange, FocalMeanOperators(), radius)(version)

  def demAspect(coverages: Seq[MultibandTileLayerRDD[SpatialKey]])
               (zoom: Int, version: String = "v1"): MultibandTileLayerRDD[SpatialKey] =
    computeOutKey[DEMWindow](coverages,
      DEMOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "aspect"), 1)(version)

  def rawDEMAspect(rawMultibandTilesWithRange: RawMultibandTilesWithMeta)
                  (zoom: Int, version: String = "v1"): RawMultibandTilesWithMeta =
    computeOutRawKey[DEMWindow](rawMultibandTilesWithRange,
      DEMOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "aspect"), 1)(version)

  def aspect(coverages: Seq[MultibandTileLayerRDD[SpatialKey]])
            (zoom: Int, version: String = "v1"): MultibandTileLayerRDD[SpatialKey] =
    computeOutKey[ASMidResult](coverages,
      AspectSlopeOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "aspect"), 1)(version)

  def rawAspect(rawMultibandTilesWithRange: RawMultibandTilesWithMeta)
               (zoom: Int, version: String = "v1"): RawMultibandTilesWithMeta =
    computeOutRawKey[ASMidResult](rawMultibandTilesWithRange,
      AspectSlopeOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "aspect"), 1)(version)

  def demSlope(coverages: Seq[MultibandTileLayerRDD[SpatialKey]])
              (zoom: Int, version: String = "v1"): MultibandTileLayerRDD[SpatialKey] =
    computeOutKey[DEMWindow](coverages,
      DEMOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "slope"), 1)(version)

  def rawDEMSlope(rawMultibandTilesWithRange: RawMultibandTilesWithMeta)
                 (zoom: Int, version: String = "v1"): RawMultibandTilesWithMeta =
    computeOutRawKey[DEMWindow](rawMultibandTilesWithRange,
      DEMOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "slope"), 1)(version)

  def slope(coverages: Seq[MultibandTileLayerRDD[SpatialKey]])
           (zoom: Int, version: String = "v1"): MultibandTileLayerRDD[SpatialKey] =
    computeOutKey[ASMidResult](coverages,
      AspectSlopeOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "slope"), 1)(version)

  def rawSlope(rawMultibandTilesWithRange: RawMultibandTilesWithMeta)
              (zoom: Int, version: String = "v1"): RawMultibandTilesWithMeta =
    computeOutRawKey[ASMidResult](rawMultibandTilesWithRange,
      AspectSlopeOperators(WEB_MERCATOR_RESOLUTION(zoom), WEB_MERCATOR_RESOLUTION(zoom), "slope"), 1)(version)
}
