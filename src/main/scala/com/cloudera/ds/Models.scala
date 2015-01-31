package com.cloudera.ds

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql._


case class PlayerGameRecord(playerId: String, game: Int, passingPts: Int,
                            rushingPts: Int, recievingPts: Int) {
  /** Returns type expected by MultivariateOnlineSummarizer. */
  def getNumericValsAsVector(): Vector = {
    Vectors.dense(Array(passingPts.toDouble, rushingPts.toDouble, recievingPts.toDouble))
  }
}

object PlayerGameRecord {
  def safeGet[T](row: Row, idx: Int, default: T) = {
    if (row.isNullAt(idx)) {
      default
    } else {
      row.getAs[T](idx)
    }
  }
  /** Convenience function for creating player game record. */
  def apply(row: Row) = {
    new PlayerGameRecord(row.getString(0), safeGet[Int](row, 1, 0), safeGet[Int](row, 2, 0),
      safeGet[Int](row, 3, 0), safeGet[Int](row, 4, 0))
  }
}

case class PlayerStats(totalGamesPlayed: Long, passingStats: StatSummary, rushingStats: StatSummary,
                       recievingStats: StatSummary, totalStats: StatSummary)

object PlayerStats {
  /** Convenience function for creating PlayerStats */
  def apply(statSummary: MultivariateOnlineSummarizer): PlayerStats = {
    val meanArr = statSummary.mean.toArray
    val varArr = statSummary.variance.toArray
    new PlayerStats(statSummary.count, new StatSummary(meanArr(0), varArr(0)),
      new StatSummary(meanArr(1), varArr(1)), new StatSummary(meanArr(2), varArr(2)),
      new StatSummary(meanArr.sum, varArr.sum))
  }
}

/** Stats to keep. */
case class StatSummary(mean: Double, variance: Double)