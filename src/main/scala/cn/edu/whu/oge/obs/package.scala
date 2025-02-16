package cn.edu.whu.oge

import cn.edu.whu.oge.obs.OBSConf.{accessKey, endpoint, secretKey}
import io.minio.MinioClient

/**
 * 对象存储包
 *
 * @author irisacsee
 * @since 2025/2/14
 */
package object obs {
  def getMinioClient: MinioClient = {
    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(endpoint)
      .credentials(accessKey, secretKey)
      .build()
    minioClient.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
    minioClient
  }
}
