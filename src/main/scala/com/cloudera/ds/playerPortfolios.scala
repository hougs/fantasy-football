package com.cloudera.ds

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import sys.process._


object playerPortfolios {

  /**
   * Returns an RDD keyed by player id and season pairs, with associated values statistics on
   * fantasy football points.
   */
  def playerSeasonStats(playerGameRecordRdd: RDD[PlayerGameRecord],
                        gameSeasonRdd: RDD[(Int, Int)]): RDD[((String, Int), PlayerStats)] = {
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
      PlayerStats(statSummarizer)
    }
  }

  def getHiveJars(): String= {
    val result: String = "find /opt/cloudera/parcels/CDH-5.3.0-1.cdh5.3.0.p0.30/lib/hive/lib/ -name" +
      " '*.jar' -not -name 'guava*'" !!
    val jars: Array[String]= result.split("\n")
    jars.mkString(",")
  }

  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("Player Portfolio Optimization")
    val jars = getHiveJars()
    println(jars)
    conf.set("spark.driver.extraClassPath", jars)
    conf.set("spark.executor.extraClassPath", jars)
    conf
  }

  /** Entry point to this Spark job. */
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(configure(master))
    val gameSeason = DataSource.gamesSeasonHiveSparkSql(sc)
    val playerGame = DataSource.playerGameRddSparkSql(sc)

    playerSeasonStats(playerGame, gameSeason). map(rec =>
      s"${rec._1._1},${rec._1._2},"
    )
  }
}
