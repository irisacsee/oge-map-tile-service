package cn.edu.whu.oge.coverage

import geotrellis.raster.{DoubleConstantNoDataCellType, IntConstantNoDataCellType, MultibandTile}

/**
 * 在不同key中计算，不同key之间相互影响的算子，MR为中间结果的类型
 *
 * @author irisacsee
 * @since 2025/2/26
 */
trait ComputeOutKeyOperators[MR] extends Serializable {
  final val NODATA: Int = IntConstantNoDataCellType.noDataValue
  final val DOUBLE_NODATA: Double = DoubleConstantNoDataCellType.noDataValue

  /**
   * 创建初始中间结果
   *
   * @return 初始中间结果
   */
  def createMr: MR

  /**
   * 从相同key的瓦片数据中获取中间结果
   *
   * @param cells  瓦片数据
   * @param mrs    中间结果
   * @param radius 邻域半径
   */
  def inKeyOp(cells: Array[Int],
              mrs: Array[MR],
              radius: Int): Unit

  /**
   * 从不同key的瓦片拷贝数据中获取中间结果
   *
   * @param cells       瓦片拷贝数据
   * @param mrs         中间结果
   * @param cellsRange  当前中间结果涉及的瓦片拷贝范围(rowMin, rowMax, colMin, colMax)
   * @param cellsMaxCol 瓦片拷贝数据最大列数
   * @param location    位置二元组，瓦片拷贝数据相对于中间结果的位置
   * @param mrsIndex    中间结果索引
   */
  def outKeyOp(cells: Array[Int],
               mrs: Array[MR],
               cellsRange: (Int, Int, Int, Int),
               cellsMaxCol: Int,
               location: (Int, Int),
               mrsIndex: Int): Unit

  /**
   * 相同key的中间结果合并
   *
   * @param src 被合并中间结果
   * @param tgt 合并中间结果
   */
  def combKeyOp(src: Array[MR],
                tgt: Array[MR]): Unit

  /**
   * 最终的转换算子
   *
   * @param midResult 中间结果
   * @return 计算完成的像元值
   */
  def finalMapOp(midResult: MR): Int

  /**
   * 针对同一key的处理算子，默认保留第一个
   *
   * @param multibandTiles 同一key的瓦片
   * @return 处理后的瓦片
   */
  def sameKeyOp(multibandTiles: Seq[MultibandTile]): MultibandTile = multibandTiles.head
}
