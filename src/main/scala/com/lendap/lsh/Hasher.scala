package com.lendap.lsh


import org.apache.spark.mllib.linalg.SparseVector
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
class Hasher(val r: Array[Double]) extends Serializable {

  /** hash SparseVector v with random vector r */
  def hash(u : SparseVector) : Int = {
    val rVec: Array[Double] = u.indices.map(i => r(i))
    val hashVal = (rVec zip u.values).map(_tuple => _tuple._1 * _tuple._2).sum
    if (hashVal > 0) 1 else 0
  }

}

object Hasher {

  /** create a new instance providing size of the random vector Array [Double] */
  def apply (size: Int, seed: Long = System.nanoTime) = new Hasher(r(size, seed))

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, seed: Long) : Array[Double] = {
    val buf = new ArrayBuffer[Double]
    val rnd = new Random(seed)
    for (i <- 0 until size)
      buf += (if (rnd.nextGaussian() < 0) -1 else 1)
    buf.toArray
  }

}
