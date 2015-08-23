package com.lendap.lsh

/**
 * Created by maruf on 09/08/15.
 */
import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext}

trait LocalSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

}
