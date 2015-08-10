package com.lendap.lsh

/**
 * Created by maytekin on 06.08.2015.
 */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

/** m: max number of elements in a vector */
class LSHModel(m : Int, numHashFunc : Int) extends Serializable {

  /** generate numHashFunc hash functions */
  private val _hashFunctions = ListBuffer[Hasher]()
  for (i <- 0 until numHashFunc)
    _hashFunctions += Hasher.create(m)
  final val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

  /** the signature matrix with (hashFunctions.size signatures) */
  var signatureMatrix : RDD[List[Int]] = null

  /** the "bands" ((hash of List, band#), row#) */
  var bands : RDD[((Int, Int), Iterable[Long])] = null

  /** (vector id, cluster id) */
  var vector_cluster : RDD[(Long, Long)] = null

  /** (cluster id, vector id) */
  var cluster_vector : RDD[(Long, Long)] = null

  /** (cluster id, List(Vector) */
  var clusters : RDD[(Long, Iterable[SparseVector])] = null

  /** filter out scores below threshold. this is an optional step.*/
  //def filter(hashKey : String) : RDD = ???

  //def compare(SparseVector v) : RDD


}

