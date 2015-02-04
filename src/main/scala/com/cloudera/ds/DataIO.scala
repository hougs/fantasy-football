package com.cloudera.ds

import com.cloudera.ds.football.avro.PlayerYearlyStats
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import parquet.avro.AvroParquetOutputFormat

object DataIO {
  /** path to player-game-points parquet file. */
  val playerGamePointsPath = "/user/hive/warehouse/super_football.db/player_game_points/"
  /** path to per Player yearly stats. */
  val yearlyStatsPath= "/user/juliet/football/playerYearStats"
  /** path to counts of players by position. */
  val countByPositionPath = "/user/juliet/football/positionCounts"

  /** SQL for select game-season data. */
  val gameSeasonSelect = "SELECT gid, seas FROM football.games"
  /** SQL to select playerid and position. */
  val playerPositionSelect = "SELECT player, pos1 FROM football.players"

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

  def playerPositionHiveSparkSql(sc: SparkContext): RDD[(String, String)] = {
    val hiveSqlContext = new HiveContext(sc)
    hiveSqlContext.sql(playerPositionSelect).map(row => (row.getString(0), row.getString(1)))
  }

  def writeScoredIn2014ToFile(scoredIn2014Rdd: RDD[PlayerYearlyStats]) = {
    val job = new Job()
    FileOutputFormat.setOutputPath(job, new Path(yearlyStatsPath))
    AvroParquetOutputFormat.setSchema(job, PlayerYearlyStats.SCHEMA$)
    job.setOutputFormatClass(classOf[AvroParquetOutputFormat])
    scoredIn2014Rdd.map((x) => (null, x)).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def writePositionCounts(rdd: RDD[(String, Int)]) = {
    rdd.map(tuple => s"${tuple._1},${tuple._2.toString}").saveAsTextFile(countByPositionPath)
  }
}
