package com.cloudera.ds

import com.cloudera.ds.football.avro.PlayerYearlyStats
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._


object PlayerPortfolios {

  /**
   * Returns an RDD keyed by player id and season pairs, with associated values statistics on
   * fantasy football points.
   */
  def playerSeasonStats(playerGameRecordRdd: RDD[PlayerGameRecord],
                        gameSeasonRdd: RDD[(Int, Int)]): RDD[((String, Int), SingleYearStats)] = {
    // name Rdd with indication of how it is keyed
    val PlayerRecordsByGID = playerGameRecordRdd.map(record => (record.game, record))
    // Elements of this Rdd are (PlayerRecord, Sea)
    val PlayerRecordSeason: RDD[(PlayerGameRecord, Int)] = PlayerRecordsByGID.join(gameSeasonRdd)
      .values
    val myPair: RDD[((String, Int), Vector)] = PlayerRecordSeason.map(element => ((element._1
      .playerId, element._2), element._1.getNumericValsAsVector()))
    val statSummarizer = myPair.aggregateByKey(new MultivariateOnlineSummarizer())((summarizer:
                                                                                    MultivariateOnlineSummarizer, value: Vector) => summarizer.add(value),
      (summarizer1: MultivariateOnlineSummarizer, summarizer2: MultivariateOnlineSummarizer) =>
        summarizer1.merge(summarizer2))
    statSummarizer.mapValues { statSummarizer: MultivariateOnlineSummarizer =>
      SingleYearStats(statSummarizer)
    }
  }

  def getHiveJars(): String = {
    val result: String = "find /opt/cloudera/parcels/CDH-5.3.0-1.cdh5.3.0.p0.30/lib/hive/lib/ " +
      "-name" +
      " '*.jar' -not -name 'guava*'" !!
    val jars: Array[String] = result.split("\n")
    jars.mkString(",")
  }

  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("Player Portfolio Optimization")
    MyKryoRegistrator.register(conf)
    conf
  }

  /**
   * @return an Rdd of PlayerYearlyStats, filtered by players who scored in 2014
   */
  def playerStatsWhoScoredIn2014(playerSeasonStats: RDD[((String, Int),
    SingleYearStats)]): RDD[PlayerYearlyStats] = {
    val seasonStatsByPlayersId: RDD[(String, (Int, SingleYearStats))] = playerSeasonStats.map[(String, (Int,
      SingleYearStats))](record => (record._1._1, (record._1._2,
      record._2)))

    val groupedSeasonStatsByPlayersId: RDD[(String, Map[Int, SingleYearStats])] =
      seasonStatsByPlayersId.groupByKey().mapValues { statsByYear: Iterable[(Int,
        SingleYearStats)] => statsByYear.toMap
      }

    val scoringPlayersOf2014 = groupedSeasonStatsByPlayersId.filter(played2014(_))
    scoringPlayersOf2014.map[PlayerYearlyStats]{record=> Avro.toPlayerYearlyStats(record)}
  }

  def played2014(record: (String, Map[Int, SingleYearStats])): Boolean = {
    val playerRecord2014 = record._2(2014)
    playerRecord2014.totalGamesPlayed != 0 && playerRecord2014.totalStats.mean != 0
  }




  /** Entry point to this Spark job. */
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(configure(master))
    val gameSeason = DataIO.gamesSeasonHiveSparkSql(sc)
    val playerGame = DataIO.playerGameRddSparkSql(sc)

    val statsByPlayerSeason = playerSeasonStats(playerGame, gameSeason)
    val scoredIn2014 = playerStatsWhoScoredIn2014(statsByPlayerSeason)
    DataIO.writeScoredIn2014ToFile(scoredIn2014)
  }
}
