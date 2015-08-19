package com.lendap.lsh

/**
 * Created by maytekin on 06.08.2015.
 */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

/** m: max number of elements in a vector */
class LSHModel(m: Int, numHashFunc : Int, numBands: Int) extends Serializable {

  /** generate numHashFunc * numBands hash functions */
  private val _hashFunctions = ListBuffer[Hasher]()
  for (i <- 0 until numHashFunc * numBands)
    _hashFunctions += Hasher.create(m)
  final val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

  /** the "bands" bandID, (hash key, vector_id list) */
  var bands : RDD[(Int,(String, Iterable[Long]))] = null

  /** filter out buckets with hashKey.*/
  def filter(hashKey : String) : RDD[(String, Iterable[Long])] = bands.filter(x => x._2._1 == hashKey).map(x => x._2)

}

