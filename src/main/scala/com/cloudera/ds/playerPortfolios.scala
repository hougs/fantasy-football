package com.cloudera.ds

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.hive.HiveContext


object playerPortfolios {
  /** path to player-game-points parquet file */
  val playerGamePointsPath = "/user/hive/warehouse/super_football.db/player_game_points/"

  /**
   * Returns an RDD keyed by player id and season pairs, with associated values statistics on
   * fantasy football points.
   */
  def playerSeasonStats(playerGameRecordRdd: RDD[PlayerGameRecord],
                        gameSeasonRdd: RDD[(Int, Long)]): RDD[((String, Long), PlayerStats)] = {
    // name Rdd with indication of how it is keyed
    val PlayerRecordsByGID = playerGameRecordRdd.map(record => (record.game, record))
    // Elements of this Rdd are (PlayerRecord, Sea)
    val PlayerRecordSeason: RDD[(PlayerGameRecord, Long)] = PlayerRecordsByGID.join(gameSeasonRdd)
      .values
    val myPair: RDD[((String, Long), Vector)] = PlayerRecordSeason.map(element => ((element._1
      .playerId, element._2), element._1.getNumericValsAsVector()))
    val statSummarizer = myPair.aggregateByKey(new MultivariateOnlineSummarizer())((summarizer:
           MultivariateOnlineSummarizer, value: Vector) => summarizer.add(value),
      (summarizer1: MultivariateOnlineSummarizer, summarizer2: MultivariateOnlineSummarizer) =>
        summarizer1.merge(summarizer2))
    statSummarizer.mapValues { statSummarizer: MultivariateOnlineSummarizer =>
      PlayerStats(statSummarizer)
    }
  }

  /**
   * Retrieve an Rdd of player-game records using spark sql.
   * @param sc SparkContext to use when generating the Rdd. Useful for unit tests.
   */
  def playerGameRddSparkSql(sc: SparkContext): RDD[PlayerGameRecord] = {
    val sqlContext = new SQLContext(sc)
    val playGamePoints = sqlContext.parquetFile(playerGamePointsPath)
    playGamePoints.map(row => PlayerGameRecord(row)).cache()
  }

  def gamesSeasonHiveSparkSql(sc: SparkContext) = {
    val hiveSqlContext = new HiveContext(sc)
    val gameSeasonPairs = hiveSqlContext.sql("FROM football.games SELECT gid, " +
      "seas").map(row => (row.getInt(0), row.getLong(1)))
  }

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "Player Portfolio Optimization")
  }
}
