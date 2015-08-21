package com.lendap.lsh

/**
 * Created by maruf on 09/08/15.
 */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

class LSH(data : RDD[(Long, SparseVector)], size: Int, numHashFunc : Int, numBands : Int) extends Serializable {

  /** Build LSH model. */
  def model() : LSHModel = {

    //create a new model object
    val model = new LSHModel(size, numHashFunc, numBands)

    val dataRDD = data.cache()

    //compute hash keys for each vector
    // - hash each vector numHashFunc times
    // - concat each hash value to create a hash key
    // - position band id hash keys and associated vector ids into a new RDD.
    val hashedDataRDD = dataRDD
      .map(v => (model.hashFunctions.map(h => (h._1.hash(v._2), h._2 % numHashFunc)), v._1))
      .map(x => x._1.map(a => ((a._2, x._2), a._1)))
      .flatMap(a => a).groupByKey()
      .map(x => ((x._1._1, x._2.mkString("")), x._1._2))
      .groupByKey().cache()

    //group items that hash together in the same bucket (band#, (hash_key, vec_id list))
    model.bands = hashedDataRDD.map(x => (x._1._1, (x._1._2, x._2))).cache()

    model
  }

  /** hash a single vector against an existing model and return the candidate buckets */
  def filter(data : SparseVector, model : LSHModel, itemID : Long) : RDD[Iterable[Long]] = {
    val hashKey = model.hashFunctions.map(h => h._1.hash(data)).mkString("")
    model.bands.filter(x => x._2._1 == hashKey).map(a => a._2._2)
  }

}
