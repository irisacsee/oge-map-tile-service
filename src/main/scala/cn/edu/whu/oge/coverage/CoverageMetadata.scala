package oge.conf.coverage

import geotrellis.proj4.CRS
import geotrellis.vector.io.readWktOrWkb
import oge.conf.coverage.OGEDataType.OGEDataType
import oge.db.simpleSelect
import org.locationtech.jts.geom.Geometry

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class CoverageMetadata(var coverageId: String,
                            var path: String,
                            var geom: Geometry,
                            var measurement: String,
                            var measurementRank: Int,
                            var time: LocalDateTime,
                            var crs: CRS,
                            var dataType: OGEDataType,
                            var resolution: Double)

object CoverageMetadata {
  def queryCoverage(coverageId: String, productKey: String): ListBuffer[CoverageMetadata] = {
    val start = System.currentTimeMillis
    val metaData = new ListBuffer[CoverageMetadata]

    // 查询数据并处理
    simpleSelect(
      resultNames = Array(
        "oge_image.product_key", "oge_image.image_identification",
        "oge_image.crs", "oge_image.path", "st_astext(oge_image.geom)"
      ),
      tableName = "oge_image",
      rangeLimit = Array(
        ("image_identification", "=", coverageId),
        ("name", "=", productKey)
      ),
      aliases = Array(
        "geom",
        "oge_image.phenomenon_time",
        "oge_data_resource_product.name",
        "oge_data_resource_product.dtype",
        "oge_product_measurement.band_num",
        "oge_product_measurement.band_rank",
        "oge_product_measurement.band_train",
        " oge_product_measurement.resolution_m"
      ),
      jointLimit = Array(
        ("oge_data_resource_product",
          "oge_image.product_key= oge_data_resource_product.id"),
        ("oge_product_measurement",
          "oge_product_measurement.product_key=oge_data_resource_product.id"),
        ("oge_measurement",
          "oge_product_measurement.measurement_key=oge_measurement.measurement_key")
      ),
      func = extentResults => {
        // 排除BQA波段
        while (extentResults.next()) {
          if (extentResults.getInt("band_train") != 0) {
            val coverageId = extentResults.getString("image_identification")
            val measurement = extentResults.getString("band_num")
            val coverageMetadata = CoverageMetadata(
              coverageId = coverageId,
              measurement = measurement,
              measurementRank = extentResults.getInt("band_rank"),
              path = s"${extentResults.getString("path")}/${coverageId}_$measurement.tif",
              geom = readWktOrWkb(extentResults.getString("geom")),
              time = extractTime(extentResults.getString("phenomenon_time")),
              crs = CRS.fromName(extentResults.getString("crs")),
              dataType = OGEDataType.withName(extentResults.getString("dtype")),
              resolution = extentResults.getDouble("resolution_m")
            )
            metaData.append(coverageMetadata)
          }
        }
      }
    )
    println(s"查询耗时：${System.currentTimeMillis - start}ms")

    metaData
  }

  private def extractTime(time: String): LocalDateTime = {
    val formatPatterns = new mutable.ArrayBuffer[String] // 日期格式模式列表
    formatPatterns.append("yyyy-MM-dd HH:mm:ss")
    formatPatterns.append("yyyy/MM/dd HH:mm:ss")
    formatPatterns.append("yyyy-MM-dd")
    formatPatterns.append("yyyy/MM/dd")

    var localDateTime: LocalDateTime = null
    var localDate: LocalDate = null
    val loop = new Breaks
    loop.breakable {
      for (pattern <- formatPatterns) {
        try {
          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
          localDateTime = LocalDateTime.parse(time, formatter)
          loop.break()
          // 匹配成功，退出循环
        } catch {
          case _: DateTimeParseException =>
            try {
              val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
              localDate = LocalDate.parse(time, formatter)
              loop.break()
              // 匹配成功，退出循环
            } catch {
              case _: DateTimeParseException =>
              // 匹配失败，继续尝试下一个模式
            }
        }
      }
    }

    if (localDateTime != null) localDateTime
    else if (localDate != null) localDate.atStartOfDay()
    else null
  }
}

object OGEDataType extends Enumeration with Serializable {
  type OGEDataType = Value

  // 定义一个包含优先级的类
  case class OGEDataValue(priority: Int) extends super.Val

  // 创建枚举成员，并为每个成员设置优先级
  val int8raw: OGEDataValue = OGEDataValue(1)
  val uint8raw: OGEDataValue = OGEDataValue(1)
  val int8: OGEDataValue = OGEDataValue(1)
  val uint8: OGEDataValue = OGEDataValue(1)
  val int16raw: OGEDataValue = OGEDataValue(1)
  val uint16raw: OGEDataValue = OGEDataValue(1)
  val int16: OGEDataValue = OGEDataValue(1)
  val uint16: OGEDataValue = OGEDataValue(1)
  val int32raw: OGEDataValue = OGEDataValue(1)
  val int32: OGEDataValue = OGEDataValue(1)
  val float32raw: OGEDataValue = OGEDataValue(2)
  val float32: OGEDataValue = OGEDataValue(2)
  val float64raw: OGEDataValue = OGEDataValue(3)
  val float64: OGEDataValue = OGEDataValue(3)

  // 定义一个比较函数来比较两个成员的优先级，并根据优先级返回相应的数据类型
  def compareAndGetType(member1: OGEDataType, member2: OGEDataType): OGEDataType = {
    val priority1: Int = member1.asInstanceOf[OGEDataValue].priority
    val priority2: Int = member2.asInstanceOf[OGEDataValue].priority

    if (priority1 == 1 && priority2 == 1) int32raw
    else if (priority1 <= 2 && priority2 <= 2) float32raw
    else float64raw
  }
}
