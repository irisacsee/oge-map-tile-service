package cn.edu.whu.oge.coverage

import cn.edu.whu.oge.coverage.CoordinateTransformer.projTileCodeToGeoExtent
import cn.edu.whu.oge.coverage.TileSerializer.deserializeTileData
import cn.edu.whu.oge.obs.OBSConf.bucketName
import geotrellis.layer.SpatialKey
import geotrellis.proj4.LatLng
import geotrellis.vector.Extent
import geotrellis.vector.reproject.Reproject
import io.minio.{GetObjectArgs, MinioClient}
import oge.conf.coverage.CoverageMetadata
import oge.conf.coverage.OGEDataType.OGEDataType
import org.locationtech.jts.geom.{Envelope, Geometry}

import java.io.{ByteArrayOutputStream, InputStream}
import scala.collection.mutable.ArrayBuffer

/**
 * COG格式解析方法
 *
 * @author irisacsee
 * @since 2025/2/14
 */
object COGParser {
  private final val COG_HEAD_SIZE: Int = 5000000 // COG文件头部大小
  private final val TYPES: Array[Int] = Array(
    0, //
    1, // byte // 8-bit unsigned integer
    1, // ascii // 8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero)
    2, // short",2), // 16-bit (2-byte) unsigned integer.
    4, // long",4), // 32-bit (4-byte) unsigned integer.
    8, // rational",8), // Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
    1, // sbyte",1), // An 8-bit signed (twos-complement) integer
    1, // undefined",1), // An 8-bit byte that may contain anything, depending on the definition of the field
    2, // sshort",1), // A 16-bit (2-byte) signed (twos-complement) integer.
    4, // slong",1), // A 32-bit (4-byte) signed (twos-complement) integer.
    8, // srational",1), //Two SLONG’s: the first represents the numerator of a fraction, the second the denominator.
    4, // float",4), // Single precision (4-byte) IEEE format
    8 // double",8) // Double precision (8-byte) IEEE format
  )
  private final val HEAD_BYTE_COUNT = 8
  private final val LANDSAT_LEVEL_TILE_COUNTS = Map(
    30 -> Array(961, 256, 64, 16, 4), 15 -> Array(3782, 961, 256, 64, 16, 4))
  final val TILE_BYTE_COUNT = 256 * 256 * 2

  var visZoom: Int = 0
  var tileDifference = 0
  var queryExtent: Extent = _

  type COGTileMeta = (CoverageMetadata,
    ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
    ArrayBuffer[Double], ArrayBuffer[Double],
    ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
    Int)

  /**
   * 根据影像元数据查询COG文件内的瓦片元数据，应用在方法1
   *
   * @param obs              对象存储客户端
   * @param zoom             当前前端层级
   * @param coverageMetadata 影像元数据
   * @param bandCounts       波段数列表
   * @return COGMeta即六元组，(影像元数据, 各瓦片字节数, cell, geoTrans, 各瓦片偏移量, 波段数)
   */
  def cogTileQueryForM1(obs: MinioClient,
                        zoom: Int,
                        coverageMetadata: CoverageMetadata,
                        bandCounts: Int*): COGTileMeta = {
    visZoom = zoom
    var bandCount = 1
    if (bandCounts.length > 1) throw new RuntimeException("bandCount 参数最多传一个")
    if (bandCounts.length == 1) bandCount = bandCounts(0)

    // 一些头文件变量
    val imageSize: ArrayBuffer[Int] = new ArrayBuffer[Int](2) // imageLength & imageWidth
    imageSize.append(0)
    imageSize.append(0)
    val tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]] = ArrayBuffer.empty[ArrayBuffer[ArrayBuffer[Long]]]
    val geoTrans: ArrayBuffer[Double] = ArrayBuffer.empty[Double]
    val cell: ArrayBuffer[Double] = ArrayBuffer.empty[Double]
    val tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]] = ArrayBuffer.empty[ArrayBuffer[ArrayBuffer[Long]]]
    val inputStream: InputStream = obs.getObject(GetObjectArgs
      .builder
      .bucket(bucketName)
      .`object`(coverageMetadata.path)
      .offset(0L)
      .length(COG_HEAD_SIZE)
      .build)
    // Read data from stream
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](COG_HEAD_SIZE)
    var len: Int = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) {
      outStream.write(buffer, 0, len)
    }
    val head: Array[Byte] = outStream.toByteArray
    outStream.close()
    inputStream.close()

    headParse(head, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount)
    (coverageMetadata, tileByteCounts, cell, geoTrans, tileOffsets, bandCount)
  }

  /**
   * 根据影像元数据查询COG文件内的瓦片元数据，应用在方法2
   *
   * @param obs              对象存储客户端
   * @param zoom             当前前端层级
   * @param coverageMetadata 影像元数据
   * @param projTileCodes    前端需要的瓦片编号集合
   * @param queryGeometry    查询几何
   * @param bandCounts       波段数列表
   * @return RawTile数组
   */
  def cogTileQueryForM2(obs: MinioClient,
                        zoom: Int,
                        coverageMetadata: CoverageMetadata,
                        projTileCodes: Array[(Int, Int)],
                        queryGeometry: Geometry,
                        bandCounts: Int*): ArrayBuffer[RawTile] = {
    visZoom = zoom
    var bandCount = 1
    if (bandCounts.length > 1) throw new RuntimeException("bandCount 参数最多传一个")
    if (bandCounts.length == 1) bandCount = bandCounts(0)

    // 一些头文件变量
    val imageSize: ArrayBuffer[Int] = new ArrayBuffer[Int](2) // imageLength & imageWidth
    imageSize.append(0)
    imageSize.append(0)
    val tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]] = ArrayBuffer.empty[ArrayBuffer[ArrayBuffer[Long]]]
    val geoTrans: ArrayBuffer[Double] = ArrayBuffer.empty[Double]
    val cell: ArrayBuffer[Double] = ArrayBuffer.empty[Double]
    val tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]] = ArrayBuffer.empty[ArrayBuffer[ArrayBuffer[Long]]]
    val inputStream: InputStream = obs.getObject(GetObjectArgs
      .builder
      .bucket(bucketName)
      .`object`(coverageMetadata.path)
      .offset(0L)
      .length(COG_HEAD_SIZE)
      .build)
    // Read data from stream
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](COG_HEAD_SIZE)
    var len: Int = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) {
      outStream.write(buffer, 0, len)
    }
    val head: Array[Byte] = outStream.toByteArray
    outStream.close()
    inputStream.close()

    headParse(head, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount)
    getCOGTilesMetaForM2(zoom, coverageMetadata, tileOffsets, cell,
      geoTrans, tileByteCounts, bandCount, projTileCodes, queryGeometry)
  }

  /**
   * 解析参数，并修改一些数据
   *
   * @param head
   * @param tileOffsets
   * @param cell
   * @param geoTrans
   * @param tileByteCounts
   * @param imageSize 图像尺寸
   */
  private def headParse(head: Array[Byte],
                        tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                        cell: ArrayBuffer[Double],
                        geoTrans: ArrayBuffer[Double],
                        tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                        imageSize: ArrayBuffer[Int],
                        bandCount: Int): Unit = {
    // DecodeIFH
    var pIFD: Int = getIntII(head, 4, 4)
    // DecodeIFD
    while (pIFD != 0) {
      val DECount: Int = getIntII(head, pIFD, 2)
      pIFD += 2
      for (_ <- 0 until DECount) {
        // DecodeDE
        val TagIndex: Int = getIntII(head, pIFD, 2)
        val TypeIndex: Int = getIntII(head, pIFD + 2, 2)
        val Count: Int = getIntII(head, pIFD + 4, 4)
        // 先找到数据的位置
        var pData: Int = pIFD + 8
        val totalSize: Int = TYPES(TypeIndex) * Count
        if (totalSize > 4) {
          pData = getIntII(head, pData, 4)
        }
        // 再根据Tag把值读出并存起来，GetDEValue
        getDEValue(TagIndex, TypeIndex, Count, pData, head, tileOffsets,
          cell, geoTrans, tileByteCounts, imageSize, bandCount)
        // 之前的
        pIFD += 12
      }
      pIFD = getIntII(head, pIFD, 4)
    }
  }

  /**
   * 获取COG瓦片元数据，应用在方法1
   *
   * @param zoom
   * @param coverageMetadata
   * @param tileOffsets
   * @param cell
   * @param geoTrans
   * @param tileByteCounts
   * @param bandCount
   * @param windowsExtent
   * @param queryGeometry
   * @return
   */
  def getCOGTilesMetaForM1(zoom: Int,
                           coverageMetadata: CoverageMetadata,
                           tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                           cell: ArrayBuffer[Double],
                           geoTrans: ArrayBuffer[Double],
                           tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                           bandCount: Int,
                           windowsExtent: Extent,
                           queryGeometry: Geometry): ArrayBuffer[(Long, Long, OGEDataType, Extent, SpatialKey)] = {
    var level: Int = 0
    var resolutionVis: Double = .0
    val resolutionOrigin: Double = coverageMetadata.resolution

    if (zoom == -1) {
      level = 0
    }
    else {
      resolutionVis = WEB_MERCATOR_RESOLUTION(zoom)
      level = Math.floor(Math.log(resolutionVis / resolutionOrigin) / Math.log(2)).toInt + 1
      if (level > tileOffsets.length - 1) {
        tileDifference = tileOffsets.length - 1 - level
        level = tileOffsets.length - 1
      }
      else if (level < 0) {
        tileDifference = -level
        level = 0
      }
    }

    // 使用窗口范围的全局变量
    val visualExtent = Reproject(windowsExtent, LatLng, coverageMetadata.crs)
    val queryEnv: Envelope = queryGeometry.getEnvelopeInternal
    val queryMbr: Extent = Reproject(queryEnv, LatLng, coverageMetadata.crs)

    // 将传入的范围改为数据所在坐标系下，方便两个范围进行相交
    // 传入的范围始终是 4326 坐标系下的
    queryExtent = Extent(queryMbr.xmin, queryMbr.ymin, queryMbr.xmax, queryMbr.ymax)

    // 图像范围
    // 东西方向空间分辨率  --->像素宽度
    val wReso: Double = cell(0)
    // 南北方向空间分辨率 ---> 像素高度
    val hReso: Double = cell(1)
    // 左上角x坐标，y坐标 ---> 影像 左上角 投影坐标
    val xMin: Double = geoTrans(3)
    val yMax: Double = geoTrans(4)

    //计算目标影像的左上和右下图上坐标
    val (pLeft, pRight, pLower, pUpper) = (
      ((queryExtent.xmin - xMin) / (256 * wReso * Math.pow(2, level).toInt)).toInt,
      ((queryExtent.xmax - xMin) / (256 * wReso * Math.pow(2, level).toInt)).toInt,
      ((yMax - queryExtent.ymax) / (256 * hReso * Math.pow(2, level).toInt)).toInt,
      ((yMax - queryExtent.ymin) / (256 * hReso * Math.pow(2, level).toInt)).toInt
    )

    val tilesMeta = ArrayBuffer.empty[(Long, Long, OGEDataType, Extent, SpatialKey)]
    for (i <- Math.max(pLower, 0) to (
      if (pUpper >= tileOffsets(level).length / bandCount) tileOffsets(level).length / bandCount - 1
      else pUpper)) {
      for (j <- Math.max(pLeft, 0) to (
        if (pRight >= tileOffsets(level)(i).length) tileOffsets(level)(i).length - 1
        else pRight)) {
        for (k <- 0 until bandCount) {
          val extent = new Extent(
            j * (256 * wReso * Math.pow(2, level)) + xMin,
            (i + 1) * (256 * -hReso * Math.pow(2, level)) + yMax,
            (j + 1) * (256 * wReso * Math.pow(2, level)) + xMin,
            i * (256 * -hReso * Math.pow(2, level)) + yMax)
          if (extent.intersects(visualExtent)) {
            val kPlus: Int = tileOffsets(level).length / bandCount
            tilesMeta.append((
              tileOffsets(level)(i + k * kPlus)(j),
              tileByteCounts(level)(i)(j),
              coverageMetadata.dataType,
              extent,
              SpatialKey(j - pLeft, i - pLower)))
          }
        }
      }
    }
    //    println(coverageMetadata.measurement, tilesMeta.size, level)
    tilesMeta
  }

  /**
   * 获取COG瓦片元数据，应用在方法1
   *
   * @param zoom
   * @param coverageMetadata
   * @param tileOffsets
   * @param cell
   * @param geoTrans
   * @param tileByteCounts
   * @param bandCount
   * @param projTileCodes
   * @param queryGeometry
   * @return
   */
  def getCOGTilesMetaForM2(zoom: Int,
                           coverageMetadata: CoverageMetadata,
                           tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                           cell: ArrayBuffer[Double],
                           geoTrans: ArrayBuffer[Double],
                           tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                           bandCount: Int,
                           projTileCodes: Array[(Int, Int)],
                           queryGeometry: Geometry): ArrayBuffer[RawTile] = {
    var level: Int = 0
    var resolutionVis: Double = .0
    val resolutionOrigin: Double = coverageMetadata.resolution

    if (zoom == -1) {
      level = 0
    }
    else {
      resolutionVis = WEB_MERCATOR_RESOLUTION(zoom)
      level = Math.floor(Math.log(resolutionVis / resolutionOrigin) / Math.log(2)).toInt + 1
      if (level > tileOffsets.length - 1) {
        tileDifference = tileOffsets.length - 1 - level
        level = tileOffsets.length - 1
      }
      else if (level < 0) {
        tileDifference = -level
        level = 0
      }
    }
//    println(s"level: $level")

    /// 使用窗口范围的全局变量
    val queryEnv: Envelope = queryGeometry.getEnvelopeInternal
    val queryMbr: Extent = Reproject(queryEnv, LatLng, coverageMetadata.crs)

    // 将传入的范围改为数据所在坐标系下，方便两个范围进行相交
    // 传入的范围始终是 4326 坐标系下的
    queryExtent = Extent(queryMbr.xmin, queryMbr.ymin, queryMbr.xmax, queryMbr.ymax)

    // 图像范围
    // 东西方向空间分辨率  --->像素宽度
    val wReso: Double = cell(0)
    // 南北方向空间分辨率 ---> 像素高度
    val hReso: Double = cell(1)
    // 左上角x坐标，y坐标 ---> 影像 左上角 投影坐标
    val xMin: Double = geoTrans(3)
    val yMax: Double = geoTrans(4)

    //计算目标影像的左上和右下图上坐标
    val (pLeft, pRight, pLower, pUpper) = (
      ((queryExtent.xmin - xMin) / (256 * wReso * Math.pow(2, level).toInt)).toInt,
      ((queryExtent.xmax - xMin) / (256 * wReso * Math.pow(2, level).toInt)).toInt,
      ((yMax - queryExtent.ymax) / (256 * hReso * Math.pow(2, level).toInt)).toInt,
      ((yMax - queryExtent.ymin) / (256 * hReso * Math.pow(2, level).toInt)).toInt
    )

    val tiles = ArrayBuffer.empty[RawTile]
    val visualExtents = projTileCodes.map { case (x, y) =>
      val key = SpatialKey(x, y)
      val visualExtent = Reproject(projTileCodeToGeoExtent(key, zoom), LatLng, coverageMetadata.crs)
//      println(s"visualExtent: $visualExtent")
      (key, visualExtent)
    }
    for (i <- Math.max(pLower, 0) to (
      if (pUpper >= tileOffsets(level).length / bandCount) tileOffsets(level).length / bandCount - 1
      else pUpper)) {
      for (j <- Math.max(pLeft, 0) to (
        if (pRight >= tileOffsets(level)(i).length) tileOffsets(level)(i).length - 1
        else pRight)) {
        for (k <- 0 until bandCount) {
          val extent = new Extent(
            j * (256 * wReso * Math.pow(2, level)) + xMin,
            (i + 1) * (256 * -hReso * Math.pow(2, level)) + yMax,
            (j + 1) * (256 * wReso * Math.pow(2, level)) + xMin,
            i * (256 * -hReso * Math.pow(2, level)) + yMax)
          val projCodes = ArrayBuffer.empty[SpatialKey]
          visualExtents.foreach { case (key, visualExtent) =>
            if (extent.intersects(visualExtent)) {
              projCodes.append(key)
            }
          }  // 找到此COG瓦片需要参与生成的可视瓦片号
          if (projCodes.nonEmpty) {
            //            println(s"e: $extent")
            val kPlus: Int = tileOffsets(level).length / bandCount
            val measurementRank = if (bandCount == 1) {
              coverageMetadata.measurementRank
            } else {
              coverageMetadata.measurementRank + bandCount
            }
            // TODO 找到对应的projCodes
            tiles.append(RawTile(
              offset = tileOffsets(level)(i + k * kPlus)(j),
              byteCount = tileByteCounts(level)(i)(j),
              extent = extent,
              rotation = geoTrans(5),
              resolutionCol = wReso * Math.pow(2, level),
              resolutionRow = hReso * Math.pow(2, level),
              spatialKey = SpatialKey(j - pLeft, i - pLower),
              coverageId = coverageMetadata.coverageId,
              path = coverageMetadata.path,
              time = coverageMetadata.time,
              measurement = coverageMetadata.measurement,
              measurementRank = measurementRank,
              crs = coverageMetadata.crs,
              dataType = coverageMetadata.dataType,
              tile = null,
              projCodes = projCodes
            ))
          }
          //          println(k,System.currentTimeMillis()-t1)
        }
      }
    }
    //    println(coverageMetadata.measurement,tiles.size,level)
    tiles
  }

  /**
   * 获取COG瓦片
   *
   * @param obs       对象存储客户端
   * @param path      影像存储路径
   * @param offset    瓦片偏移量
   * @param byteCount 瓦片字节数
   * @return 字节数组形式的COG瓦片数据
   */
  def getCOGTileBytes(obs: MinioClient,
                      path: String,
                      offset: Long,
                      byteCount: Long): Array[Byte] = {
    //    println(s"path: $path, offset: $offset, byteCount: $byteCount")
    val inputStream: InputStream = obs.getObject(GetObjectArgs
      .builder
      .bucket(bucketName)
      .`object`(path)
      .offset(offset)
      .length(byteCount)
      .build)
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](offset.toInt)
    var len = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) outStream.write(buffer, 0, len)
    val tileBytes = outStream.toByteArray
    inputStream.close()
    outStream.close()
    tileBytes
  }

  /**
   * 通过RawTile获取COG瓦片
   *
   * @param obs     对象存储客户端
   * @param rawTile 瓦片元数据
   */
  def getCOGTileByRawTile(obs: MinioClient,
                          rawTile: RawTile): Unit = {
    val inputStream: InputStream = obs.getObject(GetObjectArgs
      .builder
      .bucket(bucketName)
      .`object`(rawTile.path)
      .offset(rawTile.offset)
      .length(rawTile.byteCount)
      .build)
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](rawTile.offset.toInt)
    var len = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) outStream.write(buffer, 0, len)
    rawTile.tile = deserializeTileData("", outStream.toByteArray, 256, rawTile.dataType)
    inputStream.close()
    outStream.close()
  }

  /**
   * 通过RawTile数组获取COG瓦片
   *
   * @param obs       对象存储客户端
   * @param rawTiles  瓦片元数据数组
   * @param path      影像存储路径
   * @param startPos  起始偏移量
   * @param byteCount 瓦片字节数
   */
  def getCOGTileByRawTileArray(obs: MinioClient,
                               rawTiles: ArrayBuffer[RawTile],
                               path: String,
                               startPos: Long,
                               byteCount: Long): Unit = {
    var i = 0
    val size = rawTiles.size
    //    println(s"path: $path, offset: $startPos, byteCount: $byteCount")
    val inputStream = obs.getObject(GetObjectArgs
      .builder
      .bucket(bucketName)
      .`object`(path)
      .offset(startPos)
      .length(byteCount)
      .build)
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](byteCount.toInt)
    var len = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) outStream.write(buffer, 0, len)
    val data = outStream.toByteArray
    var k = 0
    rawTiles.foreach(rawTile => {
      i += 1
      val buffer = new Array[Byte](TILE_BYTE_COUNT)
      var j = 0
      while (j < TILE_BYTE_COUNT) {
        buffer(j) = data(k)
        k += 1
        j += 1
      }
      rawTile.tile = deserializeTileData("", buffer, 256, rawTile.dataType)
      if (i != size) {
        //        inputStream.skip(HEAD_BYTE_COUNT)
        k += HEAD_BYTE_COUNT
      }
    })
  }

  /**
   * 从字节数组中按小端序读出一个Int类型数据
   *
   * @param bytes    字节数组
   * @param startPos 起始位置
   * @param length   读取长度
   * @return Int类型数据
   */
  private def getIntII(bytes: Array[Byte], startPos: Int, length: Int): Int = {
    var value: Int = 0
    for (i <- 0 until length) {
      value |= bytes(startPos + i) << i * 8
      if (value < 0) {
        value += 256 << i * 8
      }
    }
    value
  }

  /**
   * 从字节数组中读出一个Long类型数据
   *
   * @param bytes    字节数组
   * @param startPos 起始位置
   * @param length   读取长度
   * @return Long类型数据
   */
  private def getLong(bytes: Array[Byte], startPos: Int, length: Int): Long = {
    var value: Long = 0
    for (i <- 0 until length) {
      value |= (bytes(startPos + i) & 0xff).toLong << (8 * i)
      if (value < 0) value += 256.toLong << i * 8
    }
    value
  }

  /**
   * 从字节数组中读出一个Double类型数据
   *
   * @param bytes    字节数组
   * @param startPos 起始位置
   * @param length   读取长度
   * @return Double类型数据
   */
  def getDouble(bytes: Array[Byte], startPos: Int, length: Int): Double = {
    var value: Long = 0
    for (i <- 0 until length) {
      value |= (bytes(startPos + i) & 0xff).toLong << (8 * i)
      if (value < 0) value += 256.toLong << i * 8
    }
    java.lang.Double.longBitsToDouble(value)
  }

  /**
   * 从字节数组中读出一段字符串
   *
   * @param bytes    字节数组
   * @param startPos 起始位置
   * @param length   读取长度
   * @return 字符串
   */
  private def getString(bytes: Array[Byte], startPos: Int, length: Int): String = {
    val dd = new Array[Byte](length)
    System.arraycopy(bytes, startPos, dd, 0, length)
    new String(dd)
  }

  /**
   * 解析头部数据中的各类值
   *
   * @param tagIndex
   * @param typeIndex
   * @param count
   * @param pData
   * @param head
   * @param tileOffsets
   * @param cell
   * @param geoTrans
   * @param tileByteCounts
   * @param imageSize
   * @param bandCount
   */
  private def getDEValue(tagIndex: Int,
                         typeIndex: Int,
                         count: Int,
                         pData: Int,
                         head: Array[Byte],
                         tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                         cell: ArrayBuffer[Double],
                         geoTrans: ArrayBuffer[Double],
                         tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                         imageSize: ArrayBuffer[Int],
                         bandCount: Int): Unit = {
    val typeSize: Int = TYPES(typeIndex)
    tagIndex match {
      case 256 => //ImageWidth
        imageSize(1) = getIntII(head, pData, typeSize)
      case 257 => //ImageLength
        imageSize(0) = getIntII(head, pData, typeSize)
      case 258 =>
        val BitPerSample: Int = getIntII(head, pData, typeSize)
      case 286 => //XPosition
        val xPosition: Int = getIntII(head, pData, typeSize)
      case 287 => //YPosition
        val yPosition: Int = getIntII(head, pData, typeSize)
      case 324 => //tileOffsets
        getOffsetsArray(pData, typeSize, head, tileOffsets, imageSize, bandCount)
      case 325 => //tileByteCounts
        getByteCountsArray(pData, typeSize, head, tileByteCounts, imageSize, bandCount)
      case 33550 => //  cellWidth
        getDoubleCell(pData, typeSize, count, head, cell)
      case 33922 => //geoTransform
        getDoubleTrans(pData, typeSize, count, head, geoTrans)
      case 34737 => //Spatial reference
        val crs: String = getString(head, pData, typeSize * count)
      case _ =>
//        println("不支持该tagIndex")
    }
  }

  /**
   *
   * @param startPos
   * @param typeSize
   * @param head
   * @param tileOffsets
   * @param imageSize
   * @param bandCount
   */
  private def getOffsetsArray(startPos: Int,
                              typeSize: Int,
                              head: Array[Byte],
                              tileOffsets: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                              imageSize: ArrayBuffer[Int],
                              bandCount: Int): Unit = {
    val StripOffsets: ArrayBuffer[ArrayBuffer[Long]] = ArrayBuffer.empty[ArrayBuffer[Long]]
    for (_ <- 0 until bandCount) {
      for (i <- 0 until (imageSize(0) / 256) + 1) {
        val Offsets: ArrayBuffer[Long] = ArrayBuffer.empty[Long]
        for (j <- 0 until (imageSize(1) / 256) + 1) {
          val v: Long = getLong(head, startPos + (i * ((imageSize(1) / 256) + 1) + j) * typeSize, typeSize)
          Offsets.append(v)
        }
        StripOffsets.append(Offsets)
      }
    }
    tileOffsets.append(StripOffsets)
  }

  /**
   *
   * @param startPos
   * @param typeSize
   * @param head
   * @param tileByteCounts
   * @param imageSize
   * @param bandCount
   */
  private def getByteCountsArray(startPos: Int,
                                 typeSize: Int,
                                 head: Array[Byte],
                                 tileByteCounts: ArrayBuffer[ArrayBuffer[ArrayBuffer[Long]]],
                                 imageSize: ArrayBuffer[Int],
                                 bandCount: Int): Unit = {
    val stripBytes: ArrayBuffer[ArrayBuffer[Long]] = ArrayBuffer.empty[ArrayBuffer[Long]]
    for (_ <- 0 until bandCount) {
      for (i <- 0 until (imageSize(0) / 256) + 1) {
        val tileBytes: ArrayBuffer[Long] = ArrayBuffer.empty[Long]
        for (j <- 0 until (imageSize(1) / 256) + 1) {
          val v: Long = getLong(head, startPos + (i * ((imageSize(1) / 256) + 1) + j) * typeSize, typeSize)
          tileBytes.append(v)
        }
        stripBytes.append(tileBytes)
      }
    }
    tileByteCounts.append(stripBytes)
  }

  /**
   *
   * @param startPos
   * @param typeSize
   * @param count
   * @param head
   * @param cell
   */
  private def getDoubleCell(startPos: Int,
                            typeSize: Int,
                            count: Int,
                            head: Array[Byte],
                            cell: ArrayBuffer[Double]): Unit = {
    for (i <- 0 until count) {
      val v: Double = getDouble(head, startPos + i * typeSize, typeSize)
      cell.append(v)
    }
  }

  /**
   *
   * @param startPos
   * @param typeSize
   * @param count
   * @param header
   * @param geoTrans
   */
  private def getDoubleTrans(startPos: Int,
                             typeSize: Int,
                             count: Int,
                             header: Array[Byte],
                             geoTrans: ArrayBuffer[Double]): Unit = {
    for (i <- 0 until count) {
      val v: Double = getDouble(header, startPos + i * typeSize, typeSize)
      geoTrans.append(v)
    }
  }
}
