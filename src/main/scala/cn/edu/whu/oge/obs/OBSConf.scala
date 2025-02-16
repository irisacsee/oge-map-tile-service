package cn.edu.whu.oge.obs

case object OBSConf {
  var endpoint: String = _
  var accessKey: String = _
  var secretKey: String = _
  var bucketName: String = _
  var maxConnections: Int = _

  override def toString: String = s"""endpoint: $endpoint
                                     |accessKey: $accessKey
                                     |secretKey: $secretKey
                                     |bucketName: $bucketName
                                     |maxConnections: $maxConnections""".stripMargin
}
