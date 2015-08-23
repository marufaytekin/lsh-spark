package com.lendap.lsh

/**
 * Created by maruf on 09/08/15.
 */

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
class LSH(data : RDD[(Long, SparseVector)], m: Int, numHashFunc : Int = 4, numBands : Int = 4) extends Serializable {

  def model() : LSHModel = {

    //create a new model object
    val model = new LSHModel(m, numHashFunc, numBands)

    val dataRDD = data.cache()

    //compute hash keys for each vector
    // - hash each vector numHashFunc times
    // - concat each hash value to create a hash key
    // - position band id hash keys and associated vector ids into a new RDD.
    val hashedDataRDD = dataRDD
      .map(v => (model.hashFunctions.map(h => (h._1.hash(v._2), h._2 % numBands)), v._1))
      .map(x => x._1.map(a => ((a._2, x._2), a._1)))
      .flatMap(a => a).groupByKey()
      .map(x => ((x._1._1, x._2.mkString("")), x._1._2))
      .groupByKey().cache()

    //group items that hash together in the same bucket (band#, (hash_key, vec_id list))
    model.bands = hashedDataRDD.map(x => (x._1._1, (x._1._2, x._2))).cache()

    model

  }

}
