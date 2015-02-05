package com.cloudera.ds


import com.cloudera.ds.football.avro.{StatsByYear, PlayerYearlyStats}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}

class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PlayerYearlyStats])
    kryo.register(classOf[StatsByYear])
    kryo.register(classOf[StatSummary])
  }
}

object MyKryoRegistrator {
  def register(conf: SparkConf) {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
  }
}