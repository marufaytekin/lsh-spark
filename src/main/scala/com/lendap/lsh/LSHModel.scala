package com.lendap.lsh

/**
 * Created by maytekin on 06.08.2015.
 */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

/** Create LSH model for maximum m number of elements in each vector.
  *
  * @param m max number of possible elements in a vector
  * @param numHashFunc number of hash functions
  * @param numBands number of bands. This parameter sometimes called hash tables as well.
  *
  * */
class LSHModel(m: Int, numHashFunc : Int, numBands: Int) extends Serializable {

  /** generate numHashFunc * numBands randomly generated hash functions and store them in hashFunctions */
  private val _hashFunctions = ListBuffer[Hasher]()
  for (i <- 0 until numHashFunc * numBands)
    _hashFunctions += Hasher.create(m)
  final val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

  /** the "bands" bandID, (hash key, vector_id list) */
  var bands : RDD[(Int,(String, Iterable[Long]))] = null

  /** generic filter function for bands.*/
  def filter(f:((Int,(String, Iterable[Long]))) => Boolean) : RDD[(Int,(String, Iterable[Long]))] =
    bands.map(a => a).filter(f)

  /** hash a single vector against an existing model and return the candidate buckets */
  def filter(data : SparseVector, model : LSHModel, itemID : Long) : RDD[Iterable[Long]] = {
    val hashKey = hashFunctions.map(h => h._1.hash(data)).mkString("")
    bands.filter(x => x._2._1 == hashKey).map(a => a._2._2)
  }

}

