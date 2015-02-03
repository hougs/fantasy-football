package com.cloudera.ds

import com.cloudera.ds.football.avro.{StatsByYear, PlayerYearlyStats,
StatSummary => AvroStatSummary}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql._
import collection.JavaConversions._


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


case class SingleYearStats(totalGamesPlayed: Int, passingStats: StatSummary, rushingStats: StatSummary,
                       receivingStats: StatSummary, totalStats: StatSummary)

object SingleYearStats {
  /** Convenience function for creating PlayerStats */
  def apply(statSummary: MultivariateOnlineSummarizer): SingleYearStats = {
    val meanArr = statSummary.mean.toArray
    val varArr = statSummary.variance.toArray
    new SingleYearStats(statSummary.count.toInt, new StatSummary(meanArr(0), varArr(0)),
      new StatSummary(meanArr(1), varArr(1)), new StatSummary(meanArr(2), varArr(2)),
      new StatSummary(meanArr.sum, varArr.sum))
  }
}

/** Stats to keep. */
case class StatSummary(mean: Double, variance: Double)

object Avro {
  def toStatSummary(record: StatSummary): AvroStatSummary = {
    new AvroStatSummary(record.mean, record.variance)
  }

  def toStatsByYear(year: Int, record: SingleYearStats): StatsByYear = {
    new StatsByYear(year, record.totalGamesPlayed, toStatSummary(record.passingStats),
      toStatSummary(record.receivingStats), toStatSummary(record.rushingStats),
      toStatSummary(record.totalStats))
  }

  def toPlayerYearlyStats(record: (String, Map[Int, SingleYearStats])): PlayerYearlyStats = {
    val playerId = record._1
    val statsByYear: java.util.List[StatsByYear] = record._2.map{ tup: (Int, SingleYearStats) =>
      toStatsByYear(tup._1, tup._2)
    }.toSeq
    val builder = PlayerYearlyStats.newBuilder()
    new PlayerYearlyStats()(record._1, statsByYear)
  }
}