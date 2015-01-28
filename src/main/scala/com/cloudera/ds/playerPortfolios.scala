package com.cloudera.ds

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

case class playerGameRecord(playerId: String, game: Int, passingPts: Int, rushingPts: Int, recievingPts: Int)
object playerGameRecord {
  def apply(row: Row) ={
    new playerGameRecord(row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3),
      row.getInt(4))
  }
}
object playerPortfolios {

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(master, "Player Portfolio Optimization")
    val sqlContext = new SQLContext(sc)
    val playGamePoints = sqlContext.parquetFile("/user/hive/warehouse/super_football.db/player_game_points/")
    playGamePoints.map(row => playerGameRecord(row))
  }
}
