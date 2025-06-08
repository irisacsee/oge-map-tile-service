package cn.edu.whu.oge.server

import akka.Done
import cn.edu.whu.oge.coverage.COGParser.COGTileMeta
import cn.edu.whu.oge.coverage.storePath
import oge.conf.coverage.CoverageMetadata

import java.io.File
import java.nio.file.Files
import java.util
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object CacheManager {
  type StringBytesMap = util.HashMap[String, Array[Byte]]
  type ListBufferMap = util.HashMap[String, ListBuffer[CoverageMetadata]]
  type COGTileMetaMap = util.HashMap[String, Array[(String, COGTileMeta)]]

  final val TILE_CACHE: StringBytesMap = new StringBytesMap()
  final val COG_META_CACHE: ListBufferMap = new ListBufferMap()
  final val COG_TILE_META_CACHE: COGTileMetaMap = new COGTileMetaMap()

  def getTileBytes(bytesKey: String, format: String): Future[Array[Byte]] = Future {
    val start = System.currentTimeMillis
    val bytes = TILE_CACHE.computeIfAbsent(bytesKey, key => {
      val path = new File(s"$storePath/$key.$format").toPath
      Files.readAllBytes(path)
    })
    println(s"瓦片获取耗时：${System.currentTimeMillis - start}ms")
    bytes
  }

  def loadTileBytes(layerId: String,
                    zoom: String,
                    tileKeys: Array[String],
                    tilesDict: String,
                    format: String): Future[Done] = Future {
    val start = System.currentTimeMillis
    tileKeys.foreach(tileKey => {
      val bytesKey = s"$layerId/$zoom/$format/$tileKey"
      if (!TILE_CACHE.containsKey(bytesKey)) {
        val file = new File(s"$tilesDict\\$tileKey.$format")
        if (file.exists()) {
          val path = new File(s"$tilesDict\\$tileKey.$format").toPath
          val bytes = Files.readAllBytes(path)
          TILE_CACHE.put(bytesKey, bytes)
          // TODO: put之后通知前端
        }
      }
    })
    println(s"加载数据耗时：${System.currentTimeMillis - start}ms")
    Done
  }
}
