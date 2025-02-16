package oge

import cn.edu.whu.oge.db.DBConf
import cn.edu.whu.oge.db.DBConf.{maxRetries, pwd, retryDelay, url, user}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 数据库工具包
 *
 * @author irisacsee
 * @since 2025/2/14
 */
package object db {
  /**
   * 简单的数据库查询语句，自动释放所有连接资源，无需手动 close
   *
   * @param func        处理结果集的函数，保证连接资源及时释放
   * @param resultNames 结果集字段名
   * @param tableName   表名
   * @param rangeLimit  为空则不做范围过滤
   * @param connection  为空则内部创建连接
   * @param aliases     查询结果集的别名
   * @param jointLimit  连接查询: (表名, 限制条件)
   * @return 查询结果
   */
  def simpleSelect(func: ResultSet => Unit,
                   resultNames: Array[String],
                   tableName: String,
                   rangeLimit: Array[(String, String, String)] = null,
                   connection: Connection = null,
                   aliases: Array[String] = null,
                   jointLimit: Array[(String, String)] = null)
  : Unit = {
    if (func.toString().isEmpty) return
    assert(resultNames.length > 0)
    assert(resultNames(0).nonEmpty)
    assert(tableName.nonEmpty)

    val conn: Connection = Option(connection)
      .orElse(Some(getConnection)).get
    val range: Array[(String, String, String)] = Option(rangeLimit)
      .orElse(Some(Array[(String, String, String)]())).get

    val sql = new mutable.StringBuilder()

    sql ++= "SELECT "
    resultNames.zipWithIndex.foreach {
      case (name, i) =>
        if (i == 0) sql ++= name
        else sql ++= ", " + name
    }

    // 别名(可选)
    if (aliases != null) {
      assert(aliases.nonEmpty)
      sql ++= " AS"
      aliases.zipWithIndex.foreach {
        case (alias, i) =>
          if (i == 0) sql ++= " " + alias
          else sql ++= ", " + alias
      }
    }

    sql ++= " FROM " + tableName

    // 连接查询(可选)
    if (jointLimit != null) {
      assert(jointLimit.nonEmpty)
      jointLimit.foreach {
        case (table, limit) =>
          sql ++= " JOIN " + table + " ON " + limit
      }
    }

    sql ++= " WHERE "

    val limitList = new ArrayBuffer[String]()
    range.zipWithIndex.foreach {
      case ((key, operator, value), i) =>
        if (value == null || value.isEmpty) {
          throw new IllegalArgumentException(
            "limitList have empty value!"
          )
        }
        if (i == 0) {
          sql ++= key + " " + operator + " ?"
          limitList.append(value)
        } else {
          sql ++= " AND " + key + " " + operator + " ?"
          limitList.append(value)
        }
    }
    println(sql)
    // Configure to be Read Only
    val statement: PreparedStatement = conn.prepareStatement(
      sql.toString(),
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY
    )
    limitList.foreach(println)
    limitList.zipWithIndex.foreach {
      case (value, i) => statement.setString(i + 1, value)
    }
    try {
      // 在这里处理结果集，处理结束后关闭资源
      val resultSet: ResultSet = statement.executeQuery()
      func(resultSet)
      resultSet.close()
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
   * 获取数据库连接
   *
   * @return 数据库连接
   */
  def getConnection: Connection = {
    var retries = 0
    var connection: Connection = null

    while (retries < maxRetries && connection == null) {
      try {
        connection = DriverManager.getConnection(url, user, pwd)
      } catch {
        case _: Exception =>
          retries += 1
          println(s"连接失败，重试第 $retries 次...")
          Thread.sleep(retryDelay)
      }
    }

    if (connection == null) {
      throw new RuntimeException("无法建立数据库连接")
    }

    connection
  }
}
