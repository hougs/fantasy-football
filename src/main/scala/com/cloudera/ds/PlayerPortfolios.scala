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
  /** A filter function to determine which players scored points in 2014. */
  def played2014(record: (String, Map[Int, SingleYearStats])): Boolean = {
    record._2.keySet.exists(_ == 2014)
  }
  /** Normalize player positions by possible positions in roster. */
  def translatePosToRosterPos(origPosition: String): String = origPosition match {
    case "RB" => "RB"
    case "QB" => "QB"
    case "DB" => "DEF"
    case "DL" => "DEF"
    case "K" => "K"
    case "LB" => "DEF"
    case "OL" => "OFF"
    case "TE" => "TE"
    case "WR" => "WR"
    case "LS" => "LS"
  }
  /** Function to filter out players not in draftwprthy positions. */
  def draftworthyPosition(tuple: (String, String)): Boolean = {
    tuple._2 != "OFF" && tuple._2 != "LS"
  }

  def normalizePosition(rdd: RDD[(String, String)]) = {
    rdd.mapValues(translatePosToRosterPos(_)).filter(draftworthyPosition(_))
  }

  def countByPosition(rdd: RDD[(String, String)]): RDD[(String, Int)] = {
    rdd.map(tuple => (tuple._2, 1)).reduceByKey((a, b) => a + b)
  }

  /** Entry point to this Spark job. */
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(configure(master))
    /** Read in Data */
    val gameSeason = DataIO.gamesSeasonHiveSparkSql(sc)
    val playerGame = DataIO.playerGameRddSparkSql(sc)
    val playerPosition = normalizePosition(DataIO.playerPositionHiveSparkSql(sc))

    val statsByPlayerSeason = playerSeasonStats(playerGame, gameSeason)
    val scoredIn2014 = playerStatsWhoScoredIn2014(statsByPlayerSeason)
    val positionCounts = countByPosition(playerPosition)
    /** Write out file of counts by position. */
    DataIO.writePositionCounts(positionCounts)
    /** Write out file of stats for players who scored in 2014. */
    DataIO.writeScoredIn2014ToFile(scoredIn2014)
  }
}
