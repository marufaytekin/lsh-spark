package com.lendap.lsh

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.scalatest.FunSuite

/**
 * Created by maruf on 09/08/15.
 */
class LSHTestSuit extends FunSuite with LocalSparkContext {

  test("hasher test") {

    val data = List(
      List(5.0,3.0,4.0,5.0,5.0,1.0,5.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap),
      List(1.0,2.0,1.0,5.0,1.0,5.0,1.0,4.0,1.0,3.0).zipWithIndex.map(a=>a.swap),
      List(5.0,3.0,4.0,1.0,5.0,4.0,1.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap),
      List(1.0,3.0,4.0,5.0,5.0,1.0,1.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap))

    val h = Hasher.create(10, 12345678)
    val rdd = sc.parallelize(data)

    //make sure we have 4
    assert(rdd.count() === 4)

    //convert data to RDD of SparseVector
    val vectorRDD = rdd.map(a => Vectors.sparse(a.size, a).asInstanceOf[SparseVector])

    //make sure we still have 4
    assert(vectorRDD.count() === 4)

    val hashKey = vectorRDD.map(a => h.hash(a)).collect().mkString("")

    //check if calculated hash key correct
    assert(hashKey === "1010")



  }



}
