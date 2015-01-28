package scala.com.cloudera.ds

import com.cloudera.ds.{PlayerGameRecord, playerPortfolios}
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec

class TestPlayerPortfolio extends FlatSpec {
  val sc = new SparkContext("local", "Player Portfolio Optimization")
  val playerGameRdd = sc.parallelize(List(new PlayerGameRecord("foo", 1, 0, 1, 1),
  new PlayerGameRecord("bar", 2, 0, 2, 1)))
  val pairs = playerPortfolios.playerGameStats(playerGameRdd)
  val pairsClass = pairs.getClass
  println(s"Class of pairs is [$pairsClass]")
  val elemPairsClass = pairs.first().getClass
  println(s"Class of elements of pairs is [$elemPairsClass]")

}
