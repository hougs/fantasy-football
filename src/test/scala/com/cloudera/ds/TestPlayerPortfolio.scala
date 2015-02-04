package scala.com.cloudera.ds

import com.cloudera.ds._
import org.scalatest.ShouldMatchers
import org.apache.spark.SparkContext._


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
}
