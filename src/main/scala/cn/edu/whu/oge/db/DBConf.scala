package cn.edu.whu.oge.db

case object DBConf {
  var url: String = _
  var driver: String = _
  var user: String = _
  var pwd: String = _
  var maxRetries: Int = _
  var retryDelay: Int = _

  override def toString: String = s"""url: $url
                                     |driver: $driver
                                     |user: $user
                                     |pwd: $pwd
                                     |maxRetries: $maxRetries
                                     |retryDelay: $retryDelay""".stripMargin
}
