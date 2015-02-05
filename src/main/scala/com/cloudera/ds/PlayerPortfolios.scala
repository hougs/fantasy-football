package com.cloudera.ds

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._


object PlayerPortfolios {



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
    /** Normalize different types of positions in to ones we care about in fantasy football. */
    val playerPosition: RDD[(String, String)] = Munge.normalizePosition(DataIO
      .playerPositionHiveSparkSql(sc))
    val statsByPlayerSeason: RDD[((String, Int), SingleYearStats)]  = Munge.playerSeasonStats(playerGame, gameSeason)
    val scoredIn2014: RDD[(String, Map[Int, SingleYearStats])]  = Munge.playerStatsWhoScoredIn2014(statsByPlayerSeason)
    val positionCounts: RDD[(String, Int)] = Munge.countByPosition(playerPosition)
    /** Generate all roster combinations. */
    val rosters = GeneratePortfolio.genererate(scoredIn2014, playerPosition)
    rosters.map(_.toString()).take(10).foreach(println(_))
    /** Write out file of counts by position. */
    DataIO.writePositionCounts(positionCounts)
    /** Write out file of stats for players who scored in 2014. */
    DataIO.writeScoredIn2014ToFile(scoredIn2014)
  }
}
