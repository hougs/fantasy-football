package com.cloudera.ds

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object DataSource {
  /** path to player-game-points parquet file. */
  val playerGamePointsPath = "/user/hive/warehouse/super_football.db/player_game_points/"
  /** SQL for select game-season data. */
  val gameSeasonSelect = "SELECT gid, seas FROM football.games"

  /** Create an RDD of PlayerGameRecords. */
  def playerGameRddSparkSql(sc: SparkContext): RDD[PlayerGameRecord] = {
    val sqlContext = new SQLContext(sc)
    val playGamePoints = sqlContext.parquetFile(playerGamePointsPath)
    playGamePoints.map(row => PlayerGameRecord(row)).cache()
  }
  /** Create an RDD of game-season pairs keyed by game id. */
  def gamesSeasonHiveSparkSql(sc: SparkContext): RDD[(Int, Int)] = {
    val hiveSqlContext = new HiveContext(sc)
    val gameSeasonPairs = hiveSqlContext.sql(gameSeasonSelect)
    gameSeasonPairs.map(row => (row.getInt(0), row.getInt(1)))
  }
}
