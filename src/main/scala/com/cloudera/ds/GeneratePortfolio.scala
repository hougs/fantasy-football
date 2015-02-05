package com.cloudera.ds

import com.cloudera.ds.football.avro.PlayerYearlyStats
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object GeneratePortfolio {
  /** Filter on position. */
  def positionEquals(tuple: (String, (String, Map[Int, SingleYearStats])), position: String): Boolean = {
    tuple._2._1 == position
  }

  def positionFilterTransfrom(position: String, playerPosStats: RDD[(String,(String, Map[Int,
    SingleYearStats]))]): RDD[PlayerYearlyStats] = {
      playerPosStats.filter(positionEquals(_, position)).mapValues(values => values._2)
        .map(Avro.toPlayerYearlyStats(_))
  }

  /** Filter in to a tuple of RDDs. The order of the RDDs returned is (RB, QB, TE, K, DEF, WR)*/
  def groupsToRecombine(playerStats: RDD[(String, Map[Int, SingleYearStats])], playerPosition: RDD[(String,
    String)]): Map[String, RDD[PlayerYearlyStats]] = {
    val playerPositionStats = playerPosition.join(playerStats).cache()
    val rb = positionFilterTransfrom("RB", playerPositionStats)
    val qb = positionFilterTransfrom("QB", playerPositionStats)
    val te = positionFilterTransfrom("TE", playerPositionStats)
    val k = positionFilterTransfrom("K", playerPositionStats)
    val defense = positionFilterTransfrom("DEF", playerPositionStats)
    val wr = positionFilterTransfrom("WR", playerPositionStats)
    val wrterb = wr.union(te).union(rb)
    Map(("RB", rb),("QB", qb), ("TE", te), ("K", k), ("DEF", defense), ("WR", wr), ("WR/TE/RB",
      wrterb))
  }

  /** Cartesian product these all together.  We need to choose N for the following positions:
    * N   Position
    * 1   QB
    * 2   RB
    * 2   WR
    * 1   K
    * 1   DEF
    * 1   TE
    * 1   WR/TE/RB
    */
  def combine(inputRddMap: Map[String, RDD[PlayerYearlyStats]]) = {
    inputRddMap("QB").cartesian(inputRddMap("RB"))
      .cartesian(inputRddMap("RB")).cartesian(inputRddMap("WR")).cartesian(inputRddMap("WR"))
      .cartesian(inputRddMap("K")).cartesian(inputRddMap("DEF")).cartesian(inputRddMap("TE"))
      .cartesian(inputRddMap("WR/TE/RB"))
  }

  def flatten[A](tuple: ((((((((A, A), A), A), A), A), A), A), A)): List[A] = {
    List(tuple._1._1._1._1._1._1._1._1, tuple._1._1._1._1._1._1._1._2, tuple._1._1._1._1._1._1._2,
      tuple._1._1._1._1._1._2, tuple._1._1._1._1._2, tuple._1._1._1._2, tuple._1._1._2, tuple._1._2,
      tuple._2)
  }

  /** The cartesian product of these RDDs will have players duplicated in the roster,
    * which is impossible! So, exclude the cases where we may have duplicated them.*/
  def filterDuplicatePlayers(roster: List[PlayerYearlyStats]): Boolean = {
    roster(1) != roster(2) && roster(3) != roster(4) && roster(1) != roster(8) && roster(2) !=
      roster(8) && roster(3) != roster(8) && roster(4) != roster(8)
  }

  def deduplicatePlayersFromRoster(allRosters: RDD[List[PlayerYearlyStats]]) = {
    allRosters.filter(filterDuplicatePlayers(_))
  }

  /** Generates an Rdd of all unique combinations of valid rosters for fantasy football. */
  def genererate(playerStats: RDD[(String, Map[Int, SingleYearStats])], playerPosition: RDD[(String,
    String)]): RDD[List[PlayerYearlyStats]] = {
    val mapOfRdds = groupsToRecombine(playerStats, playerPosition)
    val combined: RDD[List[PlayerYearlyStats]] = combine(mapOfRdds).map(flatten[PlayerYearlyStats]
      (_))
    deduplicatePlayersFromRoster(combined)
  }
}
