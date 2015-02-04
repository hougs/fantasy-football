package com.cloudera.ds

import com.cloudera.ds.football.avro.PlayerYearlyStats
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

object Munge {
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

  /** A filter function to determine which players scored points in 2014. */
  def played2014(record: (String, Map[Int, SingleYearStats])): Boolean = {
    if(record._2.keySet.exists(_ == 2014)){
      record._2(2014).totalGamesPlayed != 0 && record._2(2014).totalStats.mean != 0
    } else{
      false
    }
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
}
