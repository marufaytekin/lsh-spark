package com.lendap.spark.lsh

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.scalatest.FunSuite

/**
*  Created by maruf on 09/08/15.
*/
class LSHTestSuit extends FunSuite with LocalSparkContext {

  val simpleDataRDD = List(
    List(5.0,3.0,4.0,5.0,5.0,1.0,5.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap),
    List(1.0,2.0,1.0,5.0,1.0,5.0,1.0,4.0,1.0,3.0).zipWithIndex.map(a=>a.swap),
    List(5.0,3.0,4.0,1.0,5.0,4.0,1.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap),
    List(1.0,3.0,4.0,5.0,5.0,1.0,1.0,3.0,4.0,5.0).zipWithIndex.map(a=>a.swap))

  test("hasher") {

    val h = Hasher(10, 12345678)
    val rdd = sc.parallelize(simpleDataRDD)

    //make sure we have 4
    assert(rdd.count() == 4)

    //convert data to RDD of SparseVector
    val vectorRDD = rdd.map(a => Vectors.sparse(a.size, a).asInstanceOf[SparseVector])

    //make sure we still have 4
    assert(vectorRDD.count() == 4)

    val hashKey = vectorRDD.map(a => h.hash(a)).collect().mkString("")

    //check if calculated hash key correct
    assert(hashKey == "1010")

  }

  test ("lsh") {

    val numBands = 5
    val numHashFunc = 4
    val m = 50 //number of elements in each vector
    val n = 30 //number of data points (vectors)
    val rnd = new scala.util.Random

    //generate n random vectors whose elements range 1-5
    val dataRDD = List.range(1, n)
      .map(a => (a, List.fill(m)(1 + rnd.nextInt(5).toDouble).zipWithIndex.map(x => x.swap)))
    val vectorsRDD = sc.parallelize(dataRDD).map(a => (a._1.toLong, Vectors.sparse(a._2.size, a._2).asInstanceOf[SparseVector]))

    val lsh = new  LSH(vectorsRDD, m, numHashFunc, numBands)
    val model = lsh.run()

    //make sure numBands hashTables created
    assert (model.hashTables.map(a => a._1._1).collect().distinct.length == numBands)

    //make sure each key size matches with number of hash functions
    assert (model.hashTables.filter(a => a._1._2.length != numHashFunc).count == 0)

    //make sure there is no empty bucket
    assert (model.hashTables
      .map(a => (a._1._2, a._2))
      .groupByKey().filter(x => x._2.isEmpty)
      .count == 0)

    //make sure vectors are not clustered in one bucket
    assert (model.hashTables
      .map(a => (a._1._1, a._1._2))
      .groupByKey().filter(x => x._2.size == n)
      .count == 0)

    //make sure number of buckets for each hashTables is in expected range (2 - 2^numHashFunc)
    assert (model.hashTables
      .map(a => (a._1._1, a._1._2))
      .groupByKey()
      .map(a => (a._1, a._2.toList.distinct))
      .filter(a => a._2.size < 1 || a._2.size > math.pow(2, numHashFunc))
      .count == 0)

    //test save/load operations
    val temp = "target/test/" + System.currentTimeMillis().toString
    model.save(sc, temp)
    val model2 = LSHModel.load(sc, temp)

    //make sure size of saved and loaded models are the same
    assert(model.hashTables.count == model2.hashTables.count)
    assert(model.hashFunctions.size == model2.hashFunctions.size)

    //make sure loaded model produce the same hashValue with the original
    val testRDD = vectorsRDD.take(10)
    testRDD.foreach(x => assert(model.hashValue(x._2) == model2.hashValue(x._2)))

    //test cosine similarity
    val rdd = sc.parallelize(simpleDataRDD)

    //convert data to RDD of SparseVector
    val vectorRDD = rdd.map(a => Vectors.sparse(a.size, a).asInstanceOf[SparseVector])
    val a = vectorRDD.take(4)(0)
    val b = vectorRDD.take(4)(3)
    assert(lsh.cosine(a, b) === 0.9061030445113443)


  }

}
