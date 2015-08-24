package com.lendap.lsh

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.scalatest.FunSuite

/**
 * Created by maruf on 09/08/15.
 */
class LSHTestSuit extends FunSuite with LocalSparkContext {

  test("hasher") {

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

  test ("lsh") {

    val numBands = 5
    val numHashFunc = 4
    val m = 50 //number of elements in each vector
    val n = 30 //number of data points (vectors)
    val rnd = new scala.util.Random

    //generate n random vectors whose elements range 1-5
    val data = List.range(1, n).map(a => (a, List.fill(m)(1 + rnd.nextInt((5)).toDouble).zipWithIndex.map(x => x.swap)))
    val rdd = sc.parallelize(data)
    val vectorsRDD = rdd.map(a => (a._1.toLong, Vectors.sparse(a._2.size, a._2).asInstanceOf[SparseVector]))
    val lsh = new  LSH(vectorsRDD, m, numHashFunc, numBands)
    val model = lsh.run

    //make sure numBands bands created
    assert (model.bands.map(a => a._1._1).collect().distinct.size === numBands)

    //make sure each key size matches with number of hash functions
    assert (model.bands.filter(a => a._1._2.length != numHashFunc).count === 0)

    //make sure there is no empty bucket
    assert (model.bands
      .map(a => (a._1._2, a._2))
      .groupByKey().filter(x => x._2.size == 0)
      .count === 0)

    // make sure vectors are not clustered in one bucket
    assert (model.bands
      .map(a => (a._1._1, a._1._2))
      .groupByKey().filter(x => x._2.size == n)
      .count === 0)

    // make sure number of buckets for each bands is in expected range (2 - 2^numHashFunc)
    assert (model.bands
      .map(a => (a._1._1, 1))
      .reduceByKey(_ + _)
      .filter(x => x._2 < 2 || x._2 > math.pow(2, numHashFunc))
      .count === 0)

  }



}
