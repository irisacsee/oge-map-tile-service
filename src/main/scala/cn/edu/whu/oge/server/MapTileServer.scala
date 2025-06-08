package cn.edu.whu.oge.server

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.PathMatchers.Segment
import cn.edu.whu.oge.Main.{TEST_CODES_2, TEST_ZOOMS_2}
import cn.edu.whu.oge.coverage.CoordinateTransformer.{GEO_TO_PROJ, LAYOUTS}
import cn.edu.whu.oge.coverage.{focalMean, generateVisTilesByInvertedMatchV2, partitionNum, rawFocalMean, readRawTiles, rmtReprojectByShuffle, storePath, writeVisTilesSingleBand, writeVisTilesV2}
import cn.edu.whu.oge.loadConf
import cn.edu.whu.oge.server.CacheManager.{TILE_CACHE, getTileBytes, loadTileBytes}
import cn.edu.whu.oge.server.ServerConf.innerServer
import geotrellis.layer.SpatialKey

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object MapTileServer {
  def main(args: Array[String]): Unit = {
    loadConf()

    val route =
      get {
        path("hello") {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
        } ~
          path("world") {
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello world</h1>"))
          } ~
          pathPrefix("tile") {
            path(Segment / Segment / Segment / Segment) { (layerId, zoom, x, yWithFormat) =>
              val Array(y, format) = yWithFormat.split("\\.")
              println(s"当前结果总数：${TILE_CACHE.size}")
              val bytesKey = s"$layerId/$zoom/$format/$x-$y"
              val bytes = getTileBytes(bytesKey, format)
              onSuccess(bytes) { result =>
                complete(HttpEntity(ContentTypes.`application/octet-stream`, result))
              }
            }
          }
      } ~
        post {
          pathPrefix("job") {
            path("finished" / Segment / Segment / Segment) { (layerId, zoom, format) =>
              val tilesDict = s"$storePath\\$layerId\\$zoom\\$format"
              entity(as[String]) { tileKeysStr =>
                val tileKeys = tileKeysStr.split(",")
                val loaded = loadTileBytes(layerId, zoom, tileKeys, tilesDict, format)
                onComplete(loaded) { _ =>
                  complete(HttpResponse(StatusCodes.OK))
                }
              }
            } ~
              path("m2v2" / Segment / Segment) { (data, parts) =>
                val dataMode = data.toInt
                partitionNum = parts.toInt
                val start = System.currentTimeMillis
                val codes = TEST_CODES_2(dataMode)
                val rdd = generateVisTilesByInvertedMatchV2(
                  codes, "LC81220392015275LGN00", "LC08_L1T", TEST_ZOOMS_2(dataMode), Set("B1", "B3", "B4"))
                println(s"瓦片总数：${rdd.count}")
                println(s"瓦片范围：${rdd.metadata.extent}")
                println(s"Key范围：${rdd.metadata.bounds.get}")
                println(s"总耗时：${System.currentTimeMillis - start}")
                writeVisTilesV2(rdd, "multi", 11, "jpg")
                asyncSendPost(s"$innerServer/job/finished/multi/11/jpg",
                  codes.map(t => s"${t._1}-${t._2}").mkString(","))
                complete("瓦片生成完成")
              } ~
              path("executeCode") {
                // 随便生成一个layerId
                complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, UUID.randomUUID.toString))
              } ~
              path("executeDag") {
                // TODO: spatialRange处理
                complete("dag执行开始")
              } ~
              path("m2v2focalMean" / Segment / Segment / Segment) { (data, parts, version) =>
                val dataMode = data.toInt
                partitionNum = parts.toInt
                val start = System.currentTimeMillis
                val codes = TEST_CODES_2(dataMode)
                val zoom = TEST_ZOOMS_2(dataMode)
                val raw = readRawTiles(
                  codes, "LC81220392015275LGN00", "LC08_L1T", zoom, Set("B1", "B3", "B4"))
                val rdd = rmtReprojectByShuffle(rawFocalMean(raw, 1)(version), codes, zoom)
                println(s"瓦片总数：${rdd.count}")
                println(s"瓦片范围：${rdd.metadata.extent}")
                println(s"Key范围：${rdd.metadata.bounds.get}")
                println(s"总耗时：${System.currentTimeMillis - start}")
                writeVisTilesV2(rdd, "focalMean", 11, "jpg")
                asyncSendPost(s"$innerServer/job/finished/focalMean/11/jpg",
                  codes.map(t => s"${t._1}-${t._2}").mkString(","))
                complete("瓦片生成完成")
              } ~
              path("start" / Segment / Segment / Segment / Segment) { (layerId, zoomStr, format, spatialRange) =>
                val time1 = System.currentTimeMillis
                val preKey = s"$layerId/$zoomStr/$format"
                val extent = spatialRange.split(",")
                val (lngMin, latMin, lngMax, latMax) =
                  (extent(0).toDouble, extent(1).toDouble, extent(2).toDouble, extent(3).toDouble)
                val zoom = zoomStr.toInt
                val layout = LAYOUTS(zoom)
                val (xMin, yMin) = GEO_TO_PROJ(lngMin, latMin)
                val (xMax, yMax) = GEO_TO_PROJ(lngMax, latMax)
                val SpatialKey(xCodeMin, yCodeMax) = layout.mapTransform.pointToKey(xMin, yMin)
                val SpatialKey(xCodeMax, yCodeMin) = layout.mapTransform.pointToKey(xMax, yMax)
                val projCodes = ArrayBuffer[(Int, Int)]()
                for (xCode <- xCodeMin to xCodeMax; yCode <- yCodeMin to yCodeMax) {
                  val bytesKey = s"$preKey/$xCode-$yCode"
                  if (!TILE_CACHE.containsKey(bytesKey)) {
                    projCodes.append((xCode, yCode))
                  }
                }
                val projTileCodes = projCodes.toArray
                println(s"增量瓦片确认耗时：${System.currentTimeMillis - time1}ms")
                if (projTileCodes.isEmpty) {
                  complete("无增量瓦片")
                  return
                }
                val time2 = System.currentTimeMillis
                var rdd = generateVisTilesByInvertedMatchV2(
                  projTileCodes, "LC81220392015275LGN00", "LC08_L1T", zoom, Set("B1"))
//                val rdd = generateVisTilesByPipelineV2(sc, projTileCodes,
//                  "LC81220392015275LGN00", "LC08_L1T", zoom, Set("B1"))
                rdd = focalMean(Seq(rdd), 1)()
                println(s"瓦片总数：${rdd.count}")
                println(s"瓦片范围：${rdd.metadata.extent}")
                println(s"Key范围：${rdd.metadata.bounds.get}")
                println(s"总耗时：${System.currentTimeMillis - time2}")
//                writeVisTilesV2(rdd, layerId, zoom, format)
                writeVisTilesSingleBand(rdd, layerId, zoom, format)
                asyncSendPost(s"$innerServer/job/finished/$layerId/$zoom/$format",
                  projTileCodes.map(t => s"${t._1}-${t._2}").mkString(","))
                complete(projCodes.size.toString)
              }
          }
        }


    val bindingFuture = Http().bindAndHandle(route, "localhost", 19100)

    println(s"Server online at http://localhost:19100/\nPress RETURN to stop...")
    StdIn.readLine()
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }
}
