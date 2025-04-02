package cn.edu.whu.oge

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, StatusCodes}
import akka.stream.ActorMaterializer
import cn.edu.whu.oge.server.ServerConf.dataServer
import com.typesafe.config.ConfigFactory

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.URL
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}

/**
 * 服务端调用
 *
 * @author irisacsee
 * @since 2025/2/24
 */
package object server {
  val config = ConfigFactory.parseString(
    """akka {
      |  actor {
      |    default-dispatcher {
      |      fork-join-executor {
      |        parallelism-max = 256
      |      }
      |    }
      |  }
      |  http {
      |    server {
      |      max-connections = 10000
      |      max-connections-per-remote-address = 1000
      |      parsing {
      |        max-content-length = 10m
      |      }
      |      idle-timeout = 60s
      |    }
      |  }
      |}""".stripMargin)
  implicit val system = ActorSystem("mts", config)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  /**
   * 发送任务完成通知
   *
   * @param projTileCodes 瓦片编号
   */
  def jobFinished(projTileCodes: Array[(Int, Int)],
                  jobId: String,
                  zoom: Int): Unit = {
    val param = projTileCodes.map(t => s"[${t._1},${t._2}]").mkString("[", ",", "]")
    //    asyncSendPost(s"$jobServer/job/finished", param)
    asyncSendPost(s"$dataServer/job/finished/$jobId/$zoom/jpg", param)
  }

  /**
   * 异步发送POST请求
   *
   * @param url   请求地址
   * @param param 请求参数
   * @return 返回结果
   */
  def asyncSendPost(url: String, param: String): Future[String] = Future {
    var result = ""
    try {
      val realUrl = new URL(url)
      //打开和URL之间的连接
      val conn = realUrl.openConnection
      //设置通用的请求属性
      conn.setRequestProperty("accept", "*/*")
      conn.setRequestProperty("connection", "Keep-Alive")
      conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)")
      conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
      //发送POST请求必须设置如下两行
      conn.setDoOutput(true)
      conn.setDoInput(true)
      //获取URLConnection对象对应的输出流
      val out = new PrintWriter(conn.getOutputStream)
      //发送请求参数
      out.print(param)
      //flush输出流的缓冲
      out.flush()
      // 定义 BufferedReader输入流来读取URL的响应
      val in = new BufferedReader(new InputStreamReader(conn.getInputStream, "utf-8"))
      var line: String = null
      while ((line = in.readLine) != null) result += "\n" + line
    } catch {
      case e: Exception =>
        System.out.println("发送POST请求出现异常" + e)
        e.printStackTrace()
    }
    result
  }

  def syncSendPost(url: String, param: Array[Byte], retries: Int = 3): Unit = {
    def sendRequest(attempt: Int): Future[Any] = Future {
      val request = HttpRequest(
        method = HttpMethods.POST,
        uri = url,
        entity = HttpEntity(ContentTypes.`application/octet-stream`, param)
      )

      Http().singleRequest(request).flatMap { response =>
        response.status match {
          case StatusCodes.OK =>
            println("请求成功")
            Future.successful(())
          case _ =>
            println(s"响应状态：${response.status}")
            Future.failed(new Exception(s"请求失败，状态码：${response.status}"))
        }
      }
    }.recoverWith { case e =>
      println(s"请求异常：${e.getMessage}")
      if (attempt < retries) {
        println(s"尝试重试请求，第${attempt}次")
        sendRequest(attempt + 1)
      } else {
        println("重试次数达到上限，请求失败")
        Future.failed(e)
      }
    }

    Await.result(sendRequest(1), Duration(2, SECONDS))
  }
}
