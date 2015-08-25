package com.lendap.lsh

/**
 * Created by maruf on 09/08/15.
 */

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/** Build LSH model with data RDD. Hash each vector number of band times and stores in a bucket.
  *
  *
  * @param data RDD of sparse vectors with vector Ids. RDD(vec_id, SparseVector)
  * @param m max number of possible elements in a vector
  * @param numHashFunc number of hash functions
  * @param numBands number of bands. This parameter sometimes called buckets or hash tables as well.
  *
  * */
class LSH(data : RDD[(Long, SparseVector)] = null, m: Int = 0, numHashFunc : Int = 4, numBands : Int = 4) extends Serializable {

  def run() : LSHModel = {

    //create a new model object
    val model = new LSHModel(m, numHashFunc, numBands)

    val dataRDD = data.cache()

    //compute hash keys for each vector
    // - hash each vector numHashFunc times
    // - concat each hash value to create a hash key
    // - position band id hash keys and vector id into a new RDD.
    // - creates RDD of ((band#, hash_key), vec_id) tuples.
    model.bands = dataRDD
      .map(v => (model.hashFunctions.map(h => (h._1.hash(v._2), h._2 % numBands)), v._1))
      .map(x => x._1.map(a => ((a._2, x._2), a._1)))
      .flatMap(a => a).groupByKey()
      .map(x => ((x._1._1, x._2.mkString("")), x._1._2)).cache()

    model

  }

  def compare [T] (v1: T, v2: T): Double = ???


  def remove [T] (v : SparseVector) = ???

  //def load [T] (sc: SparkContext, path: String): LSHModel = {
  //  val model = new LSHModel(m, numHashFunc, numBands)
  //  None
  //}


}
