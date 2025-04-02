package cn.edu.whu

import cn.edu.whu.oge.db.DBConf
import cn.edu.whu.oge.obs.OBSConf
import cn.edu.whu.oge.server.ServerConf
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.Yaml

/**
 * OGE包对象
 *
 * @author irisacsee
 * @since 2025/2/14
 */
package object oge {
  private final val YAML = new Yaml()
  private val sparkConf: SparkConf = new SparkConf().setAppName("ods-test")
  implicit var sc: SparkContext = _

  private type JavaStringMap = java.util.Map[String, Any]

  /**
   * 加载配置
   *
   * @param runEnv 运行环境
   */
  def loadConf(runEnv: String = "local"): Unit = {
    val start = System.currentTimeMillis
    val confMap = getMapFromYaml(s"/$runEnv.yaml")
    loadDBConf(confMap.get("db").asInstanceOf[JavaStringMap])
    loadOBSConf(confMap.get("obs").asInstanceOf[JavaStringMap])
    loadServerConf(confMap.get("server").asInstanceOf[JavaStringMap])
    loadStoreConf(confMap.get("store").asInstanceOf[JavaStringMap])
    if (runEnv == "local") sparkConf.setMaster("local[*]")
    sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    println(s"配置加载耗时：${System.currentTimeMillis - start}ms")
  }

  /**
   * 将yaml配置文件解析为Map数据结构
   *
   * @param path yaml配置文件路径
   * @return Map结构的配置信息
   */
  private def getMapFromYaml(path: String): JavaStringMap =
    YAML.load(getClass.getResourceAsStream(path)).asInstanceOf[JavaStringMap]

  /**
   * 加载数据库配置
   *
   * @param dbMap 数据库配置信息Map
   */
  private def loadDBConf(dbMap: JavaStringMap): Unit = {
    DBConf.url = dbMap.get("url").asInstanceOf[String]
    DBConf.driver = dbMap.get("driver").asInstanceOf[String]
    DBConf.user = dbMap.get("user").asInstanceOf[String]
    DBConf.pwd = dbMap.get("pwd").asInstanceOf[String]
    DBConf.maxRetries = dbMap.get("maxRetries").asInstanceOf[Int]
    DBConf.retryDelay = dbMap.get("retryDelay").asInstanceOf[Int]
  }

  /**
   * 加载对象存储配置
   *
   * @param obsMap 对象存储配置信息Map
   */
  private def loadOBSConf(obsMap: JavaStringMap): Unit = {
    OBSConf.endpoint = obsMap.get("endpoint").asInstanceOf[String]
    OBSConf.accessKey = obsMap.get("accessKey").asInstanceOf[String]
    OBSConf.secretKey = obsMap.get("secretKey").asInstanceOf[String]
    OBSConf.bucketName = obsMap.get("bucketName").asInstanceOf[String]
    OBSConf.maxConnections = obsMap.get("maxConnections").asInstanceOf[Int]
  }

  /**
   * 加载服务端配置
   *
   * @param server 对象存储配置信息Map
   */
  private def loadServerConf(server: JavaStringMap): Unit = {
    ServerConf.jobServer = server.get("jobServer").asInstanceOf[String]
    ServerConf.dataServer = server.get("dataServer").asInstanceOf[String]
    ServerConf.innerServer = server.get("innerServer").asInstanceOf[String]
  }

  /**
   * 加载瓦片存储配置配置
   *
   * @param store 对象存储配置信息Map
   */
  private def loadStoreConf(store: JavaStringMap): Unit = coverage.storePath = store.get("path").asInstanceOf[String]
}
