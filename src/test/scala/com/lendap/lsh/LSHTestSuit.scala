package com.lendap.lsh

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.scalatest.FunSuite

/**
 * Created by maruf on 09/08/15.
 */
object LSHTestSuit extends FunSuite with LocalSparkContext {

  test("simple test") {


    val data = List(List(21, 25, 80, 110, 143, 443),
      List(21, 25, 80, 110, 143, 443, 8080),
      List(80, 2121, 3306, 3389, 8080, 8443),
      List(13, 17, 21, 23, 80, 137, 443, 3306, 3389))

    val hasher = Hasher.create(10, 12345678)
    val a1 = List(5.0,3.0,4.0,5.0,5.0,1.0,5.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap)
    val a2 = List(1.0,2.0,1.0,5.0,1.0,5.0,1.0,4.0,1.0,3.0).zipWithIndex.map(a=>a.swap)
    val a3 = List(1.0,3.0,4.0,1.0,5.0,4.0,1.0,3.0,4.0,1.0).zipWithIndex.map(a=>a.swap)
    val sv1 = Vectors.sparse(10, a1).asInstanceOf[SparseVector]
    val sv2 = Vectors.sparse(10, a2).asInstanceOf[SparseVector]
    val sv3 = Vectors.sparse(10, a3).asInstanceOf[SparseVector]
    val hashKey = hasher.hash(sv1).toString + hasher.hash(sv2).toString + hasher.hash(sv3).toString



    val rdd = sc.parallelize(data)

    //make sure we have 4
    assert(rdd.count() === 4)

    val vctr = rdd.map(r => (r.map(i => (i, 1.0)))).map(a => Vectors.sparse(65535, a).asInstanceOf[SparseVector])

    //make sure we still have 4
    assert(vctr.count() == 4)

    val h = Hasher.create(10, 123456)



  }



}
