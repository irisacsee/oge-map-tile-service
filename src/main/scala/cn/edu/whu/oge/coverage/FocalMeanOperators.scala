package cn.edu.whu.oge.coverage

/**
 * 均值滤波算子，中间结果为(像元值和，像元个数)
 *
 * @author irisacsee
 * @since 2025/2/26
 */
class FocalMeanOperators extends ComputeOutKeyOperators[(Int, Int)] {
  /**
   * 创建初始中间结果
   *
   * @return 初始中间结果
   */
  override def createMr: (Int, Int) = (0, 0)

  /**
   * 从相同key的瓦片数据中获取中间结果
   *
   * @param cells  瓦片数据
   * @param mrs    中间结果
   * @param radius 邻域半径
   */
  override def inKeyOp(cells: Array[Int],
                       mrs: Array[(Int, Int)],
                       radius: Int): Unit =
    if (mrs.head == null) createInKeyOp(cells, mrs, radius)
    else mergeInKeyOp(cells, mrs, radius)

  private def createInKeyOp(cells: Array[Int],
                            mrs: Array[(Int, Int)],
                            radius: Int): Unit = {
    // 首先计算前缀和，前缀和形式为(和，NODATA个数)
    val preSum = Array.ofDim[(Int, Int)](TILE_SIZE, TILE_SIZE)
    if (cells(0) != NODATA) {
      preSum(0)(0) = (cells(0), 0)
    } else {
      preSum(0)(0) = (0, 1)
    }
    for (col <- 1 until TILE_SIZE) {
      val pre = preSum(0)(col - 1)
      preSum(0)(col) =
        if (cells(col) != NODATA) (cells(col) + pre._1, pre._2)
        else (pre._1, pre._2 + 1)
    }
    for (row <- 1 until TILE_SIZE) {
      val (index, pre) = (row * TILE_SIZE, preSum(row - 1)(0))
      preSum(row)(0) =
        if (cells(index) != NODATA) (cells(index) + pre._1, pre._2)
        else (pre._1, pre._2 + 1)
    }
    // 矩形相加并减去重叠部分，完成前缀和计算
    for (row <- 1 until TILE_SIZE; col <- 1 until TILE_SIZE) {
      val (index, lt, l, t) =
        (row * TILE_SIZE + col, preSum(row - 1)(col - 1), preSum(row - 1)(col), preSum(row)(col - 1))
      preSum(row)(col) =
        if (cells(index) != NODATA) (cells(index) + l._1 - lt._1 + t._1, l._2 - lt._2 + t._2)
        else (l._1 - lt._1 + t._1, l._2 - lt._2 + t._2 + 1)
    }
    // 完成最后的计算
    val (len, ltIndex, brIndex) = (radius * 2 + 1, radius + 1, TILE_SIZE - radius)
    // 左上部分
    for (row <- 0 until ltIndex; col <- 0 until ltIndex) {
      val (bRow, rCol) = (row + radius, col + radius)
      val (sum, ndCount) = preSum(bRow)(rCol)
      mrs(row * TILE_SIZE + col) = (sum, (bRow + 1) * (rCol + 1) - ndCount)
    }
    // 左中部分
    for (row <- ltIndex until brIndex; col <- 0 until ltIndex) {
      val (tRow, bRow, rCol) = (row - radius - 1, row + radius, col + radius)
      val (sum, ndCount) =
        (preSum(bRow)(rCol)._1 - preSum(tRow)(rCol)._1, preSum(bRow)(rCol)._2 - preSum(tRow)(rCol)._2)
      mrs(row * TILE_SIZE + col) = (sum, len * (rCol + 1) - ndCount)
    }
    // 左下部分
    for (row <- brIndex until TILE_SIZE; col <- 0 until ltIndex) {
      val (tRow, rCol) = (row - radius - 1, col + radius)
      val (sum, ndCount) = (preSum(TILE_MAX_INDEX)(rCol)._1 - preSum(tRow)(rCol)._1,
        preSum(TILE_MAX_INDEX)(rCol)._2 - preSum(tRow)(rCol)._2)
      mrs(row * TILE_SIZE + col) = (sum, (TILE_MAX_INDEX - tRow) * (rCol + 1) - ndCount)
    }
    // 中上部分
    for (row <- 0 to ltIndex; col <- ltIndex until brIndex) {
      val (bRow, lCol, rCol) = (row + radius, col - radius - 1, col + radius)
      val (sum, ndCount) =
        (preSum(bRow)(rCol)._1 - preSum(bRow)(lCol)._1, preSum(bRow)(rCol)._2 - preSum(bRow)(lCol)._2)
      mrs(row * TILE_SIZE + col) = (sum, (bRow + 1) * len - ndCount)
    }
    // 中间部分
    val count = len * len
    for (row <- ltIndex until brIndex; col <- ltIndex until brIndex) {
      val (tRow, bRow, lCol, rCol) = (row - radius - 1, row + radius, col - radius - 1, col + radius)
      val (sum, ndCount) = (
        preSum(bRow)(rCol)._1 - preSum(bRow)(lCol)._1 + preSum(tRow)(lCol)._1 - preSum(tRow)(rCol)._1,
        preSum(bRow)(rCol)._2 - preSum(bRow)(lCol)._2 + preSum(tRow)(lCol)._2 - preSum(tRow)(rCol)._2)
      mrs(row * TILE_SIZE + col) = (sum, count - ndCount)
    }
    // 中下部分
    for (row <- brIndex until TILE_SIZE; col <- ltIndex until brIndex) {
      val (tRow, lCol, rCol) = (row - radius - 1, col - radius - 1, col + radius)
      val (sum, ndCount) = (
        preSum(TILE_MAX_INDEX)(rCol)._1 - preSum(TILE_MAX_INDEX)(lCol)._1
          + preSum(tRow)(lCol)._1 - preSum(tRow)(rCol)._1,
        preSum(TILE_MAX_INDEX)(rCol)._2 - preSum(TILE_MAX_INDEX)(lCol)._2
          + preSum(tRow)(lCol)._2 - preSum(tRow)(rCol)._2)
      mrs(row * TILE_SIZE + col) = (sum, (TILE_MAX_INDEX - tRow) * len - ndCount)
    }
    // 右上部分
    for (row <- 0 until ltIndex; col <- brIndex until TILE_SIZE) {
      val (bRow, lCol) = (row + radius, col - radius - 1)
      val (sum, ndCount) = (preSum(bRow)(TILE_MAX_INDEX)._1 - preSum(bRow)(lCol)._1,
        preSum(bRow)(TILE_MAX_INDEX)._2 - preSum(bRow)(lCol)._2)
      mrs(row * TILE_SIZE + col) = (sum, (bRow + 1) * (TILE_MAX_INDEX - lCol) - ndCount)
    }
    // 右中部分
    for (row <- ltIndex until brIndex; col <- brIndex until TILE_SIZE) {
      val (tRow, bRow, lCol) = (row - radius - 1, row + radius, col - radius - 1)
      val (sum, ndCount) = (
        preSum(bRow)(TILE_MAX_INDEX)._1 - preSum(bRow)(lCol)._1
          + preSum(tRow)(lCol)._1 - preSum(tRow)(TILE_MAX_INDEX)._1,
        preSum(bRow)(TILE_MAX_INDEX)._2 - preSum(bRow)(lCol)._2
          + preSum(tRow)(lCol)._2 - preSum(tRow)(TILE_MAX_INDEX)._2)
      mrs(row * TILE_SIZE + col) = (sum, len * (TILE_MAX_INDEX - lCol) - ndCount)
    }
    // 右下部分
    for (row <- brIndex until TILE_SIZE; col <- brIndex until TILE_SIZE) {
      val (tRow, lCol) = (row - radius - 1, col - radius - 1)
      val (sum, ndCount) = (
        preSum(TILE_MAX_INDEX)(TILE_MAX_INDEX)._1 - preSum(TILE_MAX_INDEX)(lCol)._1
          + preSum(tRow)(lCol)._1 - preSum(tRow)(TILE_MAX_INDEX)._1,
        preSum(TILE_MAX_INDEX)(TILE_MAX_INDEX)._2 - preSum(TILE_MAX_INDEX)(lCol)._2
          + preSum(tRow)(lCol)._2 - preSum(tRow)(TILE_MAX_INDEX)._2)
      mrs(row * TILE_SIZE + col) = (sum, (TILE_MAX_INDEX - tRow) * (TILE_MAX_INDEX - lCol) - ndCount)
    }
  }

  private def mergeInKeyOp(cells: Array[Int],
                           mrs: Array[(Int, Int)],
                           radius: Int): Unit = {
    // 首先计算前缀和，前缀和形式为(和，NODATA个数)
    val preSum = Array.ofDim[(Int, Int)](TILE_SIZE, TILE_SIZE)
    if (cells(0) != NODATA) {
      preSum(0)(0) = (cells(0), 0)
    } else {
      preSum(0)(0) = (0, 1)
    }
    for (col <- 1 until TILE_SIZE) {
      val pre = preSum(0)(col - 1)
      preSum(0)(col) =
        if (cells(col) != NODATA) (cells(col) + pre._1, pre._2)
        else (pre._1, pre._2 + 1)
    }
    for (row <- 1 until TILE_SIZE) {
      val (index, pre) = (row * TILE_SIZE, preSum(row - 1)(0))
      preSum(row)(0) =
        if (cells(index) != NODATA) (cells(index) + pre._1, pre._2)
        else (pre._1, pre._2 + 1)
    }
    // 矩形相加并减去重叠部分，完成前缀和计算
    for (row <- 1 until TILE_SIZE; col <- 1 until TILE_SIZE) {
      val (index, lt, l, t) =
        (row * TILE_SIZE + col, preSum(row - 1)(col - 1), preSum(row - 1)(col), preSum(row)(col - 1))
      preSum(row)(col) =
        if (cells(index) != NODATA) (cells(index) + l._1 - lt._1 + t._1, l._2 - lt._2 + t._2)
        else (l._1 - lt._1 + t._1, l._2 - lt._2 + t._2 + 1)
    }
    // 完成最后的计算
    val (len, ltIndex, brIndex) = (radius * 2 + 1, radius + 1, TILE_SIZE - radius)
    // 左上部分
    for (row <- 0 until ltIndex; col <- 0 until ltIndex) {
      val (bRow, rCol) = (row + radius, col + radius)
      val (sum, ndCount) = preSum(bRow)(rCol)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, (bRow + 1) * (rCol + 1) - ndCount + curr2)
    }
    // 左中部分
    for (row <- ltIndex until brIndex; col <- 0 until ltIndex) {
      val (tRow, bRow, rCol) = (row - radius - 1, row + radius, col + radius)
      val (sum, ndCount) =
        (preSum(bRow)(rCol)._1 - preSum(tRow)(rCol)._1, preSum(bRow)(rCol)._2 - preSum(tRow)(rCol)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, len * (rCol + 1) - ndCount + curr2)
    }
    // 左下部分
    for (row <- brIndex until TILE_SIZE; col <- 0 until ltIndex) {
      val (tRow, rCol) = (row - radius - 1, col + radius)
      val (sum, ndCount) = (preSum(TILE_MAX_INDEX)(rCol)._1 - preSum(tRow)(rCol)._1,
        preSum(TILE_MAX_INDEX)(rCol)._2 - preSum(tRow)(rCol)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, (TILE_MAX_INDEX - tRow) * (rCol + 1) - ndCount + curr2)
    }
    // 中上部分
    for (row <- 0 to ltIndex; col <- ltIndex until brIndex) {
      val (bRow, lCol, rCol) = (row + radius, col - radius - 1, col + radius)
      val (sum, ndCount) =
        (preSum(bRow)(rCol)._1 - preSum(bRow)(lCol)._1, preSum(bRow)(rCol)._2 - preSum(bRow)(lCol)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, (bRow + 1) * len - ndCount + curr2)
    }
    // 中间部分
    val count = len * len
    for (row <- ltIndex until brIndex; col <- ltIndex until brIndex) {
      val (tRow, bRow, lCol, rCol) = (row - radius - 1, row + radius, col - radius - 1, col + radius)
      val (sum, ndCount) = (
        preSum(bRow)(rCol)._1 - preSum(bRow)(lCol)._1 + preSum(tRow)(lCol)._1 - preSum(tRow)(rCol)._1,
        preSum(bRow)(rCol)._2 - preSum(bRow)(lCol)._2 + preSum(tRow)(lCol)._2 - preSum(tRow)(rCol)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, count - ndCount + curr2)
    }
    // 中下部分
    for (row <- brIndex until TILE_SIZE; col <- ltIndex until brIndex) {
      val (tRow, lCol, rCol) = (row - radius - 1, col - radius - 1, col + radius)
      val (sum, ndCount) = (
        preSum(TILE_MAX_INDEX)(rCol)._1 - preSum(TILE_MAX_INDEX)(lCol)._1
          + preSum(tRow)(lCol)._1 - preSum(tRow)(rCol)._1,
        preSum(TILE_MAX_INDEX)(rCol)._2 - preSum(TILE_MAX_INDEX)(lCol)._2
          + preSum(tRow)(lCol)._2 - preSum(tRow)(rCol)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, (TILE_MAX_INDEX - tRow) * len - ndCount + curr2)
    }
    // 右上部分
    for (row <- 0 until ltIndex; col <- brIndex until TILE_SIZE) {
      val (bRow, lCol) = (row + radius, col - radius - 1)
      val (sum, ndCount) = (preSum(bRow)(TILE_MAX_INDEX)._1 - preSum(bRow)(lCol)._1,
        preSum(bRow)(TILE_MAX_INDEX)._2 - preSum(bRow)(lCol)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, (bRow + 1) * (TILE_MAX_INDEX - lCol) - ndCount + curr2)
    }
    // 右中部分
    for (row <- ltIndex until brIndex; col <- brIndex until TILE_SIZE) {
      val (tRow, bRow, lCol) = (row - radius - 1, row + radius, col - radius - 1)
      val (sum, ndCount) = (
        preSum(bRow)(TILE_MAX_INDEX)._1 - preSum(bRow)(lCol)._1
          + preSum(tRow)(lCol)._1 - preSum(tRow)(TILE_MAX_INDEX)._1,
        preSum(bRow)(TILE_MAX_INDEX)._2 - preSum(bRow)(lCol)._2
          + preSum(tRow)(lCol)._2 - preSum(tRow)(TILE_MAX_INDEX)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, len * (TILE_MAX_INDEX - lCol) - ndCount + curr2)
    }
    // 右下部分
    for (row <- brIndex until TILE_SIZE; col <- brIndex until TILE_SIZE) {
      val (tRow, lCol) = (row - radius - 1, col - radius - 1)
      val (sum, ndCount) = (
        preSum(TILE_MAX_INDEX)(TILE_MAX_INDEX)._1 - preSum(TILE_MAX_INDEX)(lCol)._1
          + preSum(tRow)(lCol)._1 - preSum(tRow)(TILE_MAX_INDEX)._1,
        preSum(TILE_MAX_INDEX)(TILE_MAX_INDEX)._2 - preSum(TILE_MAX_INDEX)(lCol)._2
          + preSum(tRow)(lCol)._2 - preSum(tRow)(TILE_MAX_INDEX)._2)
      val index = row * TILE_SIZE + col
      val (curr1, curr2) = mrs(index)
      mrs(index) = (sum + curr1, (TILE_MAX_INDEX - tRow) * (TILE_MAX_INDEX - lCol) - ndCount + curr2)
    }
  }

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
  override def outKeyOp(cells: Array[Int],
                        mrs: Array[(Int, Int)],
                        cellsRange: (Int, Int, Int, Int),
                        cellsMaxCol: Int,
                        location: (Int, Int),
                        mrsIndex: Int): Unit = {
    val (rowMin, rowMax, colMin, colMax) = cellsRange
    var (mr1, mr2) = mrs(mrsIndex)
    for (row <- rowMin to rowMax; col <- colMin to colMax) {
      mr1 += cells(row * cellsMaxCol + col)
      mr2 += 1
    }
    mrs(mrsIndex) = (mr1, mr2)
  }

  /**
   * 相同key的中间结果合并
   *
   * @param src 被合并中间结果
   * @param tgt 合并中间结果
   */
  override def combKeyOp(src: Array[(Int, Int)],
                         tgt: Array[(Int, Int)]): Unit = {
    for (i <- src.indices) {
      val (src1, src2) = src(i)
      val (tgt1, tgt2) = tgt(i)
      tgt(i) = (src1 + tgt1, src2 + tgt2)
    }
  }

  /**
   * 最终的转换算子
   *
   * @param midResult 中间结果
   * @return 计算完成的像元值
   */
  override def finalMapOp(midResult: (Int, Int)): Int = {
    if (midResult._2 == 0) NODATA
    else midResult._1 / midResult._2
  }
}

object FocalMeanOperators {
  def apply(): FocalMeanOperators = new FocalMeanOperators()
}
