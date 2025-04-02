package cn.edu.whu.oge.coverage

import cn.edu.whu.oge.coverage.AspectSlopeOperators.ASMidResult
import cn.edu.whu.oge.coverage.DEMOperators.DEMWindow
import geotrellis.raster.DoubleConstantNoDataCellType

/**
 * DEM通用方法，邻域如下图所示<p/>
 * <p>e5 e2 e6</p>
 * <p>e1 e0 e3</p>
 * <p>e8 e4 e7</p>
 * 中间结果为九元组：(e1, e2, e3, e4, e5, e6, e7, e8, e0)
 *
 * @author irisacsee
 * @since 2025/2/28
 */
abstract class DEMOperators(xSize: Double, ySize: Double) extends ComputeOutKeyOperators[DEMWindow] {
  /**
   * 创建初始中间结果
   *
   * @return 初始中间结果
   */
  override def createMr: DEMWindow = (NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA)

  /**
   * 从相同key的瓦片数据中获取中间结果
   *
   * @param cells  瓦片数据
   * @param mrs    中间结果
   * @param radius 邻域半径
   */
  override def inKeyOp(cells: Array[Int], mrs: Array[DEMWindow], radius: Int): Unit = {
    // 左上部分
    mrs(0) = (NODATA, NODATA, cells(1), cells(256), NODATA, NODATA, cells(257), NODATA, cells(0))
    // 左中部分
    var (t, c, b, rt, r, rb) = (0, 256, 512, 1, 257, 513)
    while (c < 65280) {
      mrs(c) = (NODATA, cells(t), cells(r), cells(b), NODATA, cells(rt), cells(rb), NODATA, cells(c))
      t += TILE_SIZE
      c += TILE_SIZE
      b += TILE_SIZE
      rt += TILE_SIZE
      r += TILE_SIZE
      rb += TILE_SIZE
    }
    // 左下部分
    mrs(65280) = (NODATA, cells(65024), cells(65281), NODATA, NODATA, cells(65025), NODATA, NODATA, cells(65280))
    // 中上部分
    var (l, lb) = (0, 256)
    b = 257
    r = 2
    rb = 258
    for (c <- 1 until TILE_MAX_INDEX) {
      mrs(c) = (cells(l), NODATA, cells(r), cells(b), NODATA, NODATA, cells(rb), cells(lb), cells(c))
      l += 1
      lb += 1
      b += 1
      r += 1
      rb += 1
    }
    // 中间部分
    var row = 0
    while (row < 254) {
      var col = 0
      var lt = row * TILE_SIZE
      var (l, lb) = (lt + TILE_SIZE, lt + TILE_SIZE * 2)
      var (t, rt, c, r, b, rb) = (lt + 1, lt + 2, l + 1, l + 2, lb + 1, lb + 2)
      while (col < 254) {
        mrs(c) = (cells(l), cells(t), cells(r), cells(b), cells(lt), cells(rt), cells(rb), cells(lb), cells(c))
        lt += 1
        l += 1
        lb += 1
        t += 1
        c += 1
        b += 1
        rt += 1
        r += 1
        rb += 1
        col += 1
      }
      row += 1
    }
    // 中下部分
    var lt = 65024
    l = 65280
    t = 65025
    rt = 65026
    r = 65282
    for (c <- 65281 until 65535) {
      mrs(c) = (cells(l), cells(t), cells(r), NODATA, cells(lt), cells(rt), NODATA, NODATA, cells(c))
      lt += 1
      l += 1
      t += 1
      rt += 1
      r += 1
    }
    // 右上部分
    mrs(255) = (cells(254), NODATA, NODATA, cells(511), NODATA, NODATA, NODATA, cells(510), cells(255))
    // 右中部分
    lt = 254
    l = 510
    lb = 766
    t = 255
    c = 511
    b = 767
    while (c < 65535) {
      mrs(c) = (cells(l), cells(t), NODATA, cells(b), cells(lt), NODATA, NODATA, cells(lb), cells(c))
      lt += TILE_SIZE
      l += TILE_SIZE
      lb += TILE_SIZE
      t += TILE_SIZE
      c += TILE_SIZE
      b += TILE_SIZE
    }
    // 右下部分
    mrs(65535) = (cells(65534), cells(65279), NODATA, NODATA, cells(65278), NODATA, NODATA, NODATA, cells(65535))
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
                        mrs: Array[DEMWindow],
                        cellsRange: (Int, Int, Int, Int),
                        cellsMaxCol: Int,
                        location: (Int, Int),
                        mrsIndex: Int): Unit = {
    val (rowMin, rowMax, colMin, colMax) = cellsRange
    var (e1, e2, e3, e4, e5, e6, e7, e8, e0) = mrs(mrsIndex)
    location match {
      case (0, -1) =>
        val start = rowMin * cellsMaxCol + colMin
        if (colMin == 0 && colMax == 1) {
          e2 = cells(start)
          e6 = cells(start + 1)
        } else if (colMin == TILE_MAX_INDEX - 1 && colMax == TILE_MAX_INDEX) {
          e5 = cells(start)
          e2 = cells(start + 1)
        } else {
          e5 = cells(start)
          e2 = cells(start + 1)
          e6 = cells(start + 2)
        } // 上
      case (0, 1) =>
        val start = rowMin * cellsMaxCol + colMin
        if (colMin == 0 && colMax == 1) {
          e4 = cells(start)
          e7 = cells(start + 1)
        } else if (colMin == TILE_MAX_INDEX - 1 && colMax == TILE_MAX_INDEX) {
          e8 = cells(start)
          e4 = cells(start + 1)
        } else {
          e8 = cells(start)
          e4 = cells(start + 1)
          e7 = cells(start + 2)
        } // 下
      case (-1, 0) =>
        val start = rowMin * cellsMaxCol + colMin
        if (rowMin == 0 && rowMax == 1) {
          e1 = cells(start)
          e8 = cells(start + cellsMaxCol)
        } else if (rowMin == TILE_MAX_INDEX - 1 && rowMax == TILE_MAX_INDEX) {
          e5 = cells(start)
          e1 = cells(start + cellsMaxCol)
        } else {
          e5 = cells(start)
          e1 = cells(start + cellsMaxCol)
          e8 = cells(start + cellsMaxCol * 2)
        } // 左
      case (1, 0) =>
        val start = rowMin * cellsMaxCol + colMin
        if (rowMin == 0 && rowMax == 1) {
          e3 = cells(start)
          e7 = cells(start + cellsMaxCol)
        } else if (rowMin == TILE_MAX_INDEX - 1 && rowMax == TILE_MAX_INDEX) {
          e6 = cells(start)
          e3 = cells(start + cellsMaxCol)
        } else {
          e6 = cells(start)
          e3 = cells(start + cellsMaxCol)
          e7 = cells(start + cellsMaxCol * 2)
        } // 右
      case (-1, -1) => e5 = cells(rowMin * cellsMaxCol + colMin) // 左上
      case (1, -1) => e6 = cells(rowMin * cellsMaxCol + colMin) // 右上
      case (-1, 1) => e8 = cells(rowMin * cellsMaxCol + colMin) // 左下
      case (1, 1) => e7 = cells(rowMin * cellsMaxCol + colMin) // 右下
    }
    mrs(mrsIndex) = (e1, e2, e3, e4, e5, e6, e7, e8, e0)
  }

  /**
   * 相同key的中间结果合并
   *
   * @param src 被合并中间结果
   * @param tgt 合并中间结果
   */
  override def combKeyOp(src: Array[DEMWindow], tgt: Array[DEMWindow]): Unit = {
    for (i <- src.indices) {
      val (se1, se2, se3, se4, se5, se6, se7, se8, _) = src(i)
      var (te1, te2, te3, te4, te5, te6, te7, te8, te0) = tgt(i)
      if (se1 != NODATA) te1 = se1
      if (se2 != NODATA) te2 = se2
      if (se3 != NODATA) te3 = se3
      if (se4 != NODATA) te4 = se4
      if (se5 != NODATA) te5 = se5
      if (se6 != NODATA) te6 = se6
      if (se7 != NODATA) te7 = se7
      if (se8 != NODATA) te8 = se8
      tgt(i) = (te1, te2, te3, te4, te5, te6, te7, te8, te0)
    }
  }

  def computeDx(e: DEMWindow): Double =
    if (e._1 == NODATA || e._3 == NODATA || e._5 == NODATA || e._6 == NODATA || e._7 == NODATA || e._8 == NODATA) {
      DOUBLE_NODATA
    } else {
      ((e._6 + 2 * e._3 + e._7) - (e._5 + 2 * e._1 + e._8)) / xSize
    }

  def computeDy(e: DEMWindow): Double =
    if (e._2 == NODATA || e._4 == NODATA || e._5 == NODATA || e._6 == NODATA || e._7 == NODATA || e._8 == NODATA) {
      DOUBLE_NODATA
    } else {
      ((e._8 + 2 * e._4 + e._7) - (e._5 + 2 * e._2 + e._6)) / ySize
    }
}

/**
 * 坡向算子，用于处理DEM数据
 *
 * @author irisacsee
 * @since 2025/2/28
 */
class DEMAspectOperators(xSize: Double, ySize: Double) extends DEMOperators(xSize, ySize) {
  /**
   * 最终的转换算子，计算坡向
   *
   * @param midResult 中间结果
   * @return 计算完成的像元值
   */
  override def finalMapOp(midResult: DEMWindow): Int = {
    val (dx, dy) = (computeDx(midResult), computeDy(midResult))
    if (dx == DOUBLE_NODATA || dy == DOUBLE_NODATA) {
      NODATA
    } else {
      (1 / math.tan(dy / dx)).toInt
    }
  }
}

/**
 * 坡度算子，用于处理DEM数据
 *
 * @author irisacsee
 * @since 2025/2/28
 */
class DEMSlopeOperators(xSize: Double, ySize: Double) extends DEMOperators(xSize, ySize) {
  /**
   * 最终的转换算子，计算坡度
   *
   * @param midResult 中间结果
   * @return 计算完成的像元值
   */
  override def finalMapOp(midResult: DEMWindow): Int = {
    val (dx, dy) = (computeDx(midResult), computeDy(midResult))
    if (dx == DOUBLE_NODATA || dy == DOUBLE_NODATA) {
      NODATA
    } else {
      ((dx / dy) * 100).toInt
    }
  }
}

object DEMOperators {
  type DEMWindow = (Int, Int, Int, Int, Int, Int, Int, Int, Int)

  def apply(xSize: Double, ySize: Double, operatorName: String): DEMOperators = operatorName match {
    case "aspect" => new DEMAspectOperators(xSize, ySize)
    case "slope" => new DEMSlopeOperators(xSize, ySize)
    case _ => throw new Exception("DEMOperator not found")
  }
}

/**
 * 坡度坡向专用算子，邻域如下图所示<p/>
 * <p>e5 e2 e6</p>
 * <p>e1 e0 e3</p>
 * <p>e8 e4 e7</p>
 * 中间结果为九元组：(2 * (e3 - e1), 2 * (e4 - e2), e7 - e5, e8 - e6)</p>
 * 则dx = (t1 + t3 - t4) / xSize, dy = (t2 + t3 + t4) / ySize<p/>
 *
 * @author irisacsee
 * @since 2025/3/8
 */
abstract class AspectSlopeOperators(xSize: Double, ySize: Double) extends ComputeOutKeyOperators[ASMidResult] {
  /**
   * 创建初始中间结果
   *
   * @return 初始中间结果
   */
  override def createMr: ASMidResult = (0, 0, 0, 0)

  /**
   * 从相同key的瓦片数据中获取中间结果
   *
   * @param cells  瓦片数据
   * @param mrs    中间结果
   * @param radius 邻域半径
   */
  override def inKeyOp(cells: Array[Int],
                       mrs: Array[ASMidResult],
                       radius: Int): Unit =
    if (mrs.head == null) createInKeyOp(cells, mrs, radius)
    else mergeInKeyOp(cells, mrs, radius)

  private def createInKeyOp(cells: Array[Int],
                            mrs: Array[ASMidResult],
                            radius: Int): Unit = {
    // 左上部分
    var (e1, e2, e3, e4, e5, e6, e7, e8, e0) = (0, 0, cells(1), cells(256), 0, 0, cells(257), 0, cells(0))
    mrs(0) = if (e3 != NODATA && e4 != NODATA && e7 != NODATA && e0 != NODATA) {
      (2 * e3, 2 * e4, e7, 0)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
    // 左中部分
    var (t, c, b, rt, r, rb) = (0, 256, 512, 1, 257, 513)
    while (c < 65280) {
      e2 = cells(t)
      e3 = cells(r)
      e4 = cells(b)
      e6 = cells(rt)
      e7 = cells(rb)
      e0 = cells(c)
      mrs(c) = if (e2 != NODATA && e3 != NODATA && e4 != NODATA && e6 != NODATA && e7 != NODATA && e0 != NODATA) {
        (2 * e3, 2 * (e4 - e2), e7, -e6)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      t += TILE_SIZE
      c += TILE_SIZE
      b += TILE_SIZE
      rt += TILE_SIZE
      r += TILE_SIZE
      rb += TILE_SIZE
    }
    // 左下部分
    e2 = cells(65024)
    e3 = cells(65281)
    e6 = cells(65025)
    e0 = cells(65280)
    mrs(65280) = if (e2 != NODATA && e3 != NODATA && e6 != NODATA && e0 != NODATA) {
      (0, 2 * (e4 - e2), 0, -e6)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
    // 中上部分
    var (l, lb) = (0, 256)
    b = 257
    r = 2
    rb = 258
    for (c <- 1 until TILE_MAX_INDEX) {
      e1 = cells(l)
      e3 = cells(r)
      e4 = cells(b)
      e7 = cells(rb)
      e8 = cells(lb)
      e0 = cells(c)
      mrs(c) = if (e1 != NODATA && e3 != NODATA && e4 != NODATA && e7 != NODATA && e8 != NODATA && e0 != NODATA) {
        (2 * (e3 - e1), 2 * e4, e7, e8)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      l += 1
      lb += 1
      b += 1
      r += 1
      rb += 1
    }
    // 中间部分
    var row = 0
    while (row < 254) {
      var col = 0
      var lt = row * TILE_SIZE
      var (l, lb) = (lt + TILE_SIZE, lt + TILE_SIZE * 2)
      var (t, rt, c, r, b, rb) = (lt + 1, lt + 2, l + 1, l + 2, lb + 1, lb + 2)
      while (col < 254) {
        e1 = cells(l)
        e2 = cells(t)
        e3 = cells(r)
        e4 = cells(b)
        e5 = cells(lt)
        e6 = cells(rt)
        e7 = cells(rb)
        e8 = cells(lb)
        e0 = cells(c)
        mrs(c) = if (e1 != NODATA && e2 != NODATA && e3 != NODATA && e4 != NODATA && e5 != NODATA
          && e6 != NODATA && e7 != NODATA && e8 != NODATA && e0 != NODATA) {
          (2 * (e3 - e1), 2 * (e4 - e2), e7 - e5, e8 - e6)
        } else {
          (NODATA, NODATA, NODATA, NODATA)
        }
        lt += 1
        l += 1
        lb += 1
        t += 1
        c += 1
        b += 1
        rt += 1
        r += 1
        rb += 1
        col += 1
      }
      row += 1
    }
    // 中下部分
    var lt = 65024
    l = 65280
    t = 65025
    rt = 65026
    r = 65282
    for (c <- 65281 until 65535) {
      e1 = cells(l)
      e2 = cells(t)
      e3 = cells(r)
      e5 = cells(lt)
      e6 = cells(rt)
      e0 = cells(c)
      mrs(c) = if (e1 != NODATA && e2 != NODATA && e3 != NODATA && e5 != NODATA && e6 != NODATA && e0 != NODATA) {
        (2 * (e3 - e1), -2 * e2, -e5, -e6)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      lt += 1
      l += 1
      t += 1
      rt += 1
      r += 1
    }
    // 右上部分
    e1 = cells(254)
    e4 = cells(511)
    e8 = cells(510)
    e0 = cells(255)
    mrs(255) = if (e1 != NODATA && e4 != NODATA && e8 != NODATA && e0 != NODATA) {
      (-2 * e1, 2 * e4, 0, e8)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
    // 右中部分
    lt = 254
    l = 510
    lb = 766
    t = 255
    c = 511
    b = 767
    while (c < 65535) {
      e1 = cells(l)
      e2 = cells(t)
      e4 = cells(b)
      e5 = cells(lt)
      e8 = cells(lb)
      e0 = cells(c)
      mrs(c) = if (e1 != NODATA && e2 != NODATA && e4 != NODATA && e5 != NODATA && e8 != NODATA && e0 != NODATA) {
        (-2 * e1, 2 * (e4 - e2), -e5, e8)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      lt += TILE_SIZE
      l += TILE_SIZE
      lb += TILE_SIZE
      t += TILE_SIZE
      c += TILE_SIZE
      b += TILE_SIZE
    }
    // 右下部分
    e1 = cells(65534)
    e2 = cells(65279)
    e5 = cells(65278)
    e0 = cells(65535)
    mrs(65535) = if (e1 != NODATA && e2 != NODATA && e5 != NODATA && e0 != NODATA) {
      (-2 * e1, -2 * e2, -e5, 0)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
  }

  private def mergeInKeyOp(cells: Array[Int],
                           mrs: Array[ASMidResult],
                           radius: Int): Unit = {
    // 左上部分
    var (e1, e2, e3, e4, e5, e6, e7, e8, e0) = (0, 0, cells(1), cells(256), 0, 0, cells(257), 0, cells(0))
    mrs(0) = if (e3 != NODATA && e4 != NODATA && e7 != NODATA && e0 != NODATA) {
      (mrs(0)._1 + 2 * e3, mrs(0)._2 + 2 * e4, mrs(0)._3 + e7, mrs(0)._4)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
    // 左中部分
    var (t, c, b, rt, r, rb) = (0, 256, 512, 1, 257, 513)
    while (c < 65280) {
      e2 = cells(t)
      e3 = cells(r)
      e4 = cells(b)
      e6 = cells(rt)
      e7 = cells(rb)
      e0 = cells(c)
      mrs(c) = if (e2 != NODATA && e3 != NODATA && e4 != NODATA && e6 != NODATA && e7 != NODATA && e0 != NODATA) {
        (mrs(c)._1 + 2 * e3, 2 * (e4 - e2), mrs(c)._3 + e7, mrs(c)._4 - e6)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      t += TILE_SIZE
      c += TILE_SIZE
      b += TILE_SIZE
      rt += TILE_SIZE
      r += TILE_SIZE
      rb += TILE_SIZE
    }
    // 左下部分
    e2 = cells(65024)
    e3 = cells(65281)
    e6 = cells(65025)
    e0 = cells(65280)
    mrs(65280) = if (e2 != NODATA && e3 != NODATA && e6 != NODATA && e0 != NODATA) {
      (mrs(65280)._1, 2 * (e4 - e2), mrs(65280)._3, mrs(65280)._4 - e6)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
    // 中上部分
    var (l, lb) = (0, 256)
    b = 257
    r = 2
    rb = 258
    for (c <- 1 until TILE_MAX_INDEX) {
      e1 = cells(l)
      e3 = cells(r)
      e4 = cells(b)
      e7 = cells(rb)
      e8 = cells(lb)
      e0 = cells(c)
      mrs(c) = if (e1 != NODATA && e3 != NODATA && e4 != NODATA && e7 != NODATA && e8 != NODATA && e0 != NODATA) {
        (2 * (e3 - e1), mrs(c)._2 + 2 * e4, mrs(c)._3 + e7, mrs(c)._4 + e8)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      l += 1
      lb += 1
      b += 1
      r += 1
      rb += 1
    }
    // 中间部分
    var row = 0
    while (row < 254) {
      var col = 0
      var lt = row * TILE_SIZE
      var (l, lb) = (lt + TILE_SIZE, lt + TILE_SIZE * 2)
      var (t, rt, c, r, b, rb) = (lt + 1, lt + 2, l + 1, l + 2, lb + 1, lb + 2)
      while (col < 254) {
        e1 = cells(l)
        e2 = cells(t)
        e3 = cells(r)
        e4 = cells(b)
        e5 = cells(lt)
        e6 = cells(rt)
        e7 = cells(rb)
        e8 = cells(lb)
        e0 = cells(c)
        mrs(c) = if (e1 != NODATA && e2 != NODATA && e3 != NODATA && e4 != NODATA && e5 != NODATA
          && e6 != NODATA && e7 != NODATA && e8 != NODATA && e0 != NODATA) {
          (2 * (e3 - e1), 2 * (e4 - e2), e7 - e5, e8 - e6)
        } else {
          (NODATA, NODATA, NODATA, NODATA)
        }
        lt += 1
        l += 1
        lb += 1
        t += 1
        c += 1
        b += 1
        rt += 1
        r += 1
        rb += 1
        col += 1
      }
      row += 1
    }
    // 中下部分
    var lt = 65024
    l = 65280
    t = 65025
    rt = 65026
    r = 65282
    for (c <- 65281 until 65535) {
      e1 = cells(l)
      e2 = cells(t)
      e3 = cells(r)
      e5 = cells(lt)
      e6 = cells(rt)
      e0 = cells(c)
      mrs(c) = if (e1 != NODATA && e2 != NODATA && e3 != NODATA && e5 != NODATA && e6 != NODATA && e0 != NODATA) {
        (2 * (e3 - e1), mrs(c)._2 - 2 * e2, mrs(c)._3 - e5, mrs(c)._4 - e6)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      lt += 1
      l += 1
      t += 1
      rt += 1
      r += 1
    }
    // 右上部分
    e1 = cells(254)
    e4 = cells(511)
    e8 = cells(510)
    e0 = cells(255)
    mrs(255) = if (e1 != NODATA && e4 != NODATA && e8 != NODATA && e0 != NODATA) {
      (mrs(255)._1 - 2 * e1, mrs(255)._2 + 2 * e4, mrs(255)._3, mrs(255)._4 + e8)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
    }
    // 右中部分
    lt = 254
    l = 510
    lb = 766
    t = 255
    c = 511
    b = 767
    while (c < 65535) {
      e1 = cells(l)
      e2 = cells(t)
      e4 = cells(b)
      e5 = cells(lt)
      e8 = cells(lb)
      e0 = cells(c)
      mrs(c) = if (e1 != NODATA && e2 != NODATA && e4 != NODATA && e5 != NODATA && e8 != NODATA && e0 != NODATA) {
        (mrs(c)._1 - 2 * e1, 2 * (e4 - e2), mrs(c)._3 - e5, mrs(c)._4 + e8)
      } else {
        (NODATA, NODATA, NODATA, NODATA)
      }
      lt += TILE_SIZE
      l += TILE_SIZE
      lb += TILE_SIZE
      t += TILE_SIZE
      c += TILE_SIZE
      b += TILE_SIZE
    }
    // 右下部分
    e1 = cells(65534)
    e2 = cells(65279)
    e5 = cells(65278)
    e0 = cells(65535)
    mrs(65535) = if (e1 != NODATA && e2 != NODATA && e5 != NODATA && e0 != NODATA) {
      (mrs(65535)._1 - 2 * e1, mrs(65535)._2 - 2 * e2, mrs(65535)._3 - e5, mrs(65535)._4)
    } else {
      (NODATA, NODATA, NODATA, NODATA)
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
                        mrs: Array[ASMidResult],
                        cellsRange: (Int, Int, Int, Int),
                        cellsMaxCol: Int,
                        location: (Int, Int),
                        mrsIndex: Int): Unit = {
    val (rowMin, rowMax, colMin, colMax) = cellsRange
    var (t1, t2, t3, t4) = mrs(mrsIndex)
    if (t1 != NODATA && t2 != NODATA && t3 != NODATA && t4 != NODATA) {
      location match {
        case (0, -1) =>
          val start = rowMin * cellsMaxCol + colMin
          if (colMin == 0 && colMax == 1) {
            val e2 = cells(start)
            val e6 = cells(start + 1)
            t2 = if (e2 != NODATA) t2 - 2 * e2 else NODATA
            t4 = if (e6 != NODATA) t4 - e6 else NODATA
          } else if (colMin == TILE_MAX_INDEX - 1 && colMax == TILE_MAX_INDEX) {
            val e5 = cells(start)
            val e2 = cells(start + 1)
            t3 = if (e5 != NODATA) t3 - e5 else NODATA
            t2 = if (e2 != NODATA) t2 - 2 * e2 else NODATA
          } else {
            val e5 = cells(start)
            val e2 = cells(start + 1)
            val e6 = cells(start + 2)
            t3 = if (e5 != NODATA) t3 - e5 else NODATA
            t2 = if (e2 != NODATA) t2 - 2 * e2 else NODATA
            t4 = if (e6 != NODATA) t4 - e6 else NODATA
          } // 上
        case (0, 1) =>
          val start = rowMin * cellsMaxCol + colMin
          if (colMin == 0 && colMax == 1) {
            val e4 = cells(start)
            val e7 = cells(start + 1)
            t2 = if (e4 != NODATA) t2 + 2 * e4 else NODATA
            t3 = if (e7 != NODATA) t3 + e7 else NODATA
          } else if (colMin == TILE_MAX_INDEX - 1 && colMax == TILE_MAX_INDEX) {
            val e8 = cells(start)
            val e4 = cells(start + 1)
            t4 = if (e8 != NODATA) t4 + e8 else NODATA
            t2 = if (e4 != NODATA) t2 + 2 * e4 else NODATA
          } else {
            val e8 = cells(start)
            val e4 = cells(start + 1)
            val e7 = cells(start + 2)
            t4 = if (e8 != NODATA) t4 + e8 else NODATA
            t2 = if (e4 != NODATA) t2 + 2 * e4 else NODATA
            t3 = if (e7 != NODATA) t3 + e7 else NODATA
          } // 下
        case (-1, 0) =>
          val start = rowMin * cellsMaxCol + colMin
          if (rowMin == 0 && rowMax == 1) {
            val e1 = cells(start)
            val e8 = cells(start + cellsMaxCol)
            t1 = if (e1 != NODATA) t1 - 2 * e1 else NODATA
            t4 = if (e8 != NODATA) t4 + e8 else NODATA
          } else if (rowMin == TILE_MAX_INDEX - 1 && rowMax == TILE_MAX_INDEX) {
            val e5 = cells(start)
            val e1 = cells(start + cellsMaxCol)
            t3 = if (e5 != NODATA) t3 - e5 else NODATA
            t1 = if (e1 != NODATA) t1 - 2 * e1 else NODATA
          } else {
            val e5 = cells(start)
            val e1 = cells(start + cellsMaxCol)
            val e8 = cells(start + cellsMaxCol * 2)
            t3 = if (e5 != NODATA) t3 - e5 else NODATA
            t1 = if (e1 != NODATA) t1 - 2 * e1 else NODATA
            t4 = if (e8 != NODATA) t4 + e8 else NODATA
          } // 左
        case (1, 0) =>
          val start = rowMin * cellsMaxCol + colMin
          if (rowMin == 0 && rowMax == 1) {
            val e3 = cells(start)
            val e7 = cells(start + cellsMaxCol)
            t1 = if (e3 != NODATA) t1 + 2 * e3 else NODATA
            t3 = if (e7 != NODATA) t3 + e7 else NODATA
          } else if (rowMin == TILE_MAX_INDEX - 1 && rowMax == TILE_MAX_INDEX) {
            val e6 = cells(start)
            val e3 = cells(start + cellsMaxCol)
            t4 = if (e6 != NODATA) t4 - e6 else NODATA
            t1 = if (e3 != NODATA) t1 + 2 * e3 else NODATA
          } else {
            val e6 = cells(start)
            val e3 = cells(start + cellsMaxCol)
            val e7 = cells(start + cellsMaxCol * 2)
            t4 = if (e6 != NODATA) t4 - e6 else NODATA
            t1 = if (e3 != NODATA) t1 + 2 * e3 else NODATA
            t3 = if (e7 != NODATA) t3 + e7 else NODATA
          } // 右
        case (-1, -1) => {
          val e5 = cells(rowMin * cellsMaxCol + colMin)
          t3 = if (e5 != NODATA) t3 - e5 else NODATA
        } // 左上
        case (1, -1) => {
          val e6 = cells(rowMin * cellsMaxCol + colMin)
          t4 = if (e6 != NODATA) t4 - e6 else NODATA
        } // 右上
        case (-1, 1) => {
          val e8 = cells(rowMin * cellsMaxCol + colMin)
          t4 = if (e8 != NODATA) t4 + e8 else NODATA
        } // 左下
        case (1, 1) => {
          val e7 = cells(rowMin * cellsMaxCol + colMin)
          t3 = if (e7 != NODATA) t3 + e7 else NODATA
        } // 右下
      }
      mrs(mrsIndex) = (t1, t2, t3, t4)
    }
  }

  /**
   * 相同key的中间结果合并
   *
   * @param src 被合并中间结果
   * @param tgt 合并中间结果
   */
  override def combKeyOp(src: Array[ASMidResult], tgt: Array[ASMidResult]): Unit = {
    for (i <- src.indices) {
      val (s1, s2, s3, s4) = src(i)
      var (t1, t2, t3, t4) = tgt(i)
      if (s1 != NODATA) t1 = s1
      if (s2 != NODATA) t2 = s2
      if (s3 != NODATA) t3 = s3
      if (s4 != NODATA) t4 = s4
      tgt(i) = (t1, t2, t3, t4)
    }
  }

  def computeDx(t: ASMidResult): Double =
    if (t._1 == NODATA || t._3 == NODATA || t._4 == NODATA) {
      DOUBLE_NODATA
    } else {
      (t._1 + t._3 - t._4) / xSize
    }

  def computeDy(t: ASMidResult): Double =
    if (t._2 == NODATA || t._3 == NODATA || t._4 == NODATA) {
      DOUBLE_NODATA
    } else {
      (t._2 + t._3 + t._4) / ySize
    }
}

/**
 * 坡向算子，用于处理DEM数据
 *
 * @author irisacsee
 * @since 2025/2/28
 */
class AspectOperators(xSize: Double, ySize: Double) extends AspectSlopeOperators(xSize, ySize) {
  /**
   * 最终的转换算子，计算坡向
   *
   * @param midResult 中间结果
   * @return 计算完成的像元值
   */
  override def finalMapOp(midResult: ASMidResult): Int = {
    val (dx, dy) = (computeDx(midResult), computeDy(midResult))
    if (dx == DOUBLE_NODATA || dy == DOUBLE_NODATA) {
      NODATA
    } else {
      (1 / math.tan(dy / dx)).toInt
    }
  }
}

/**
 * 坡度算子，用于处理DEM数据
 *
 * @author irisacsee
 * @since 2025/2/28
 */
class SlopeOperators(xSize: Double, ySize: Double) extends AspectSlopeOperators(xSize, ySize) {
  /**
   * 最终的转换算子，计算坡度
   *
   * @param midResult 中间结果
   * @return 计算完成的像元值
   */
  override def finalMapOp(midResult: ASMidResult): Int = {
    val (dx, dy) = (computeDx(midResult), computeDy(midResult))
    if (dx == DOUBLE_NODATA || dy == DOUBLE_NODATA) {
      NODATA
    } else {
      ((dx / dy) * 100).toInt
    }
  }
}

object AspectSlopeOperators {
  type ASMidResult = (Int, Int, Int, Int)

  def apply(xSize: Double, ySize: Double, operatorName: String): AspectSlopeOperators = operatorName match {
    case "aspect" => new AspectOperators(xSize, ySize)
    case "slope" => new SlopeOperators(xSize, ySize)
    case _ => throw new Exception("AspectSlopeOperator not found")
  }
}
