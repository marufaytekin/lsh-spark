package com.lendap.lsh

/**
 * Created by maruf on 09/08/15.
 */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

class LSH(data : RDD[(Long, SparseVector)], size: Int, numHashFunc : Int, numBands : Int) extends Serializable {

  /** run LSH using the constructor parameters */
  def run() : LSHModel = {

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

    //we only want groups of size >= <minClusterSize>
    //(vector id, cluster id)
    //model.vector_cluster = model.bands.filter(x => x._2.size >= minClusterSize).map(x => x._2.toList.sorted).distinct().zipWithIndex().map(x => x._1.map(y => (y.asInstanceOf[Long], x._2))).flatMap(x => x.grouped(1)).map(x => x(0)).cache()

    //(cluster id, vector id)
    ///model.cluster_vector = model.vector_cluster.map(x => x.swap).cache()

    //(cluster id, List(vector))
    ///model.clusters = zdata.map(x => x.swap).join(model.vector_cluster).map(x => (x._2._2, x._2._1)).groupByKey().cache()

    model
  }

  /** compute a single vector against an existing model and return the candidate band */
  def getCandidateSets(data : SparseVector, model : LSHModel, itemID : Long) : RDD[Iterable[Long]] = {
    val hashKey = model.hashFunctions.map(h => h._1.hash(data)).mkString("")
    model.bands.filter(x => x._2._1 == hashKey).map(a => a._2._2)
  }

  /** compute jaccard between two vectors */
  def jaccard(a : SparseVector, b : SparseVector) : Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    al.intersect(bl).size / al.union(bl).size.doubleValue
  }

  /** compute jaccard similarity over a list of vectors */
  def jaccard(l : List[SparseVector]) : Double = {
    l.foldLeft(l(0).indices.toList)((a1, b1) => a1.intersect(b1.indices.toList.asInstanceOf[List[Nothing]])).size /
      l.foldLeft(List())((a1, b1) => a1.union(b1.indices.toList.asInstanceOf[List[Nothing]])).distinct.size.doubleValue
  }

}
