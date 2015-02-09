package scala.com.cloudera.ds

import com.cloudera.ds._
import com.cloudera.ds.football.avro.PlayerYearlyStats
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.scalatest.ShouldMatchers


class TestPlayerPortfolio extends SparkTestUtils with ShouldMatchers {

  sparkTest("Test playerSeasonState method") {
    val playerGameRdd = sc.parallelize(List(new PlayerGameRecord("foo", 1, 0, 1, 1),
      new PlayerGameRecord("foo", 1, 0, 1, 3),
      new PlayerGameRecord("bar", 2, 0, 2, 1)))
    val gameSeasonRdd = sc.parallelize(List((1, 2001), (2, 2002)))
    val pairs: scala.collection.Map[(String, Int), SingleYearStats] = Munge
      .playerSeasonStats(playerGameRdd, gameSeasonRdd).collectAsMap()
    pairs should have size 2
    pairs should contain key (("foo", 2001))
    pairs(("foo", 2001)) should equal(SingleYearStats(2, StatSummary(0, 0), StatSummary(1, 0),
      StatSummary(2, 2), StatSummary(3, 2)))
    pairs should contain key (("bar", 2002))
    pairs(("bar", 2002)) should equal(SingleYearStats(1, StatSummary(0, 0), StatSummary(2, 0),
      StatSummary(1, 0), StatSummary(3, 0)))
  }

  sparkTest("Test top player filtering.") {
    val scoredIn2014: RDD[(String, Map[Int, SingleYearStats])] = sc.parallelize(List(("A",
      Map(2014 -> SingleYearStats(2, StatSummary(0, 0), StatSummary(1, 0),
        StatSummary(2, 2), StatSummary(3, 2)))),
      ("B", Map(2014 -> SingleYearStats(2, StatSummary(0, 0), StatSummary(1, 0),
        StatSummary(2, 2), StatSummary(3, 2)))),
      ("C", Map(2014 -> SingleYearStats(2, StatSummary(0, 0), StatSummary(1, 0),
        StatSummary(2, 2), StatSummary(3, 2))))))

    val playerPosition: RDD[(String, String)] = sc.parallelize(List(("A", "RB"), ("B", "RB"),
      ("C", "DB")))
    val normalizedPos = Munge.normalizePosition(playerPosition)
    val normalizedPositions: scala.collection.Map[String, String] = normalizedPos.collectAsMap()
    normalizedPositions should have size 3
    normalizedPositions should contain key "A"
    normalizedPositions("A") should equal("RB")
    normalizedPositions should contain key "C"
    normalizedPositions("C") should equal("DEF")
    val topPlayersPerPos: Map[String, Array[PlayerYearlyStats]] = GeneratePortfolio
      .topPerformersByPosition(scoredIn2014, normalizedPos)
    print(topPlayersPerPos)
  }

}
