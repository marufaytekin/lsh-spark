package com.lendap.lsh


import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
 * Simple hashing function implements random hyperplane based hash functions described in
 * http://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CharikarEstim.pdf
 * r is a random vector. Hash function h_r(u) operates as follows:
 * if r.u < 0 //dot product of two vectors
 *    h_r(u) = 0
 *  else
 *    h_r(u) = 1
 */
class Hasher(r: RDD[Double]) extends Serializable {

  /** hash SparseVector v with random vector r */
  def hash(u : SparseVector) : Int = {
    //val rVec: Array[Double] = u.indices.map(i => r(i))
    //val hashVal = (rVec zip u.values).map(_tuple => _tuple._1 * _tuple._2).sum
    //if (hashVal > 0) 1 else 0
    val (indices, values) = r.zipWithIndex()
    val sv = Vectors.sparse(r.count.toInt, indices, values).asInstanceOf[SparseVector]
    for i<- u.indices

}

object Hasher {

  /** create a new instance providing size of the random vector Array [Double] */
  def create (size: Int, sc: SparkContext, numPartitions: Int) = new Hasher(r(size, sc, numPartitions))

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, sc: SparkContext, numPartitions: Int) : RDD[Double] =
    RandomRDDs.normalRDD(sc, size, numPartitions).map(x=> Math.pow(-1.0, (x*10).toInt))


}
