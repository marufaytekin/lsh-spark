package com.lendap.lsh

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.mllib.linalg.SparseVector

/**
 * Created by maytekin on 06.08.2015.
 */
class Hasher(a: SparseVector) extends Serializable {

  /** hash SparseVector v with random sparse vector a and return 0 or 1 */
  def hash(v : SparseVector) : Int = {
    val a1 = a.indices.toList
    val a2 = v.indices.toList
    val indices = a1.intersect(a2)
    val hashVal = indices.map(i => v(i) * a(i)).sum
    if (hashVal > 0) 1 else 0
  }

}

object Hasher {

  def create (min: Int, max: Int, size: Int) =
    new Hasher(randVector(min, max, size))

  /** create a random vector whose elements randomly generated integers in [min,max] range */
  def randVector(min: Int, max: Int, size: Int) : SparseVector = {
    val buf = new ArrayBuffer[Double]
    for (i <- 0 until size)
      buf += min + new Random().nextInt(max - min + 1)
    new SparseVector(buf.size, new Range(0, size, 1).toArray, buf.toArray)
  }

}
