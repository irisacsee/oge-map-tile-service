package cn.edu.whu.oge.coverage

import geotrellis.raster.MultibandTile

/**
 * 在同一个key中计算，不同key之间相互不影响的算子
 *
 * @author irisacsee
 * @since 2025/2/24
 */
object ComputeInKeyOperators {
  def addOp(multibandTiles: Seq[MultibandTile]): MultibandTile =
    multibandTiles.reduce((mt1, mt2) => MultibandTile(
      mt1.bands.zip(mt2.bands).map { case (t1, t2) => t1.localAdd(t2) }))

  /**
   * 归一化
   *
   * @param multibandTiles 需要计算的瓦片，注意该Seq的元素个数必须为1，且瓦片只有两个波段
   * @return 归一化后的瓦片
   */
  def normalizedDifferenceOp(multibandTiles: Seq[MultibandTile]): MultibandTile = {
    val bands = multibandTiles.head.bands
    val (b1, b2) = (bands(0), bands(1))
    MultibandTile(b1.localSubtract(b2).localDivide(b1.localAdd(b2)))
  }
}
