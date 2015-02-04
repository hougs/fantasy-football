package com.cloudera.ds

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object GeneratePortfolio {
  /** Filter on position. */
  def positionEquals(tuple: (String, (String, Map[Int, SingleYearStats])), position: String): Boolean = {
    tuple._2._1 == position
  }

  /** Filter in to a tuple of RDDs. The order of the RDDs returned is (RB, QB, TE, K, DEF, WR)*/
  def groupsToRecombine(playerStats: RDD[(String, Map[Int, SingleYearStats])], playerPosition: RDD[(String,
    String)]): Map[String, RDD[(String, Map[Int, SingleYearStats])]] = {
    val playerPositionStats = playerPosition.join(playerStats).cache()
    val rb = playerPositionStats.filter(positionEquals(_, "RB")).mapValues(values => values._2)
    val qb = playerPositionStats.filter(positionEquals(_,"QB")).mapValues(values => values._2)
    val te = playerPositionStats.filter(positionEquals(_,"TE")).mapValues(values => values._2)
    val k = playerPositionStats.filter(positionEquals(_,"K")).mapValues(values => values._2)
    val defense = playerPositionStats.filter(positionEquals(_,"DEF")).mapValues(values => values._2)
    val wr = playerPositionStats.filter(positionEquals(_,"WR")).mapValues(values => values._2)
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
  def combine(inputRddMap: Map[String, RDD[(String, Map[Int, SingleYearStats])]]) = {
    val rbDoubles = inputRddMap("RB").cartesian(inputRddMap("RB"))
    val wrDoubles = inputRddMap("WR").cartesian(inputRddMap("WR"))
    inputRddMap("QB").cartesian(rbDoubles).cartesian(wrDoubles).cartesian(inputRddMap("K"))
      .cartesian(inputRddMap("DEF")).cartesian(inputRddMap("TE")).cartesian(inputRddMap("WR/TE/RB"))
  }

  /** Generates an Rdd of all unqiue combinations of valid rosters for fantasy football. */
  def genererate(playerStats: RDD[(String, Map[Int, SingleYearStats])], playerPosition: RDD[(String,
    String)]) = {
    val mapOfRdds = groupsToRecombine(playerStats, playerPosition)
    combine(mapOfRdds).distinct()
  }
}
