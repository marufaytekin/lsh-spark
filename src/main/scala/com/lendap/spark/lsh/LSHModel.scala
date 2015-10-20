package com.lendap.spark.lsh

/**
 * Created by maytekin on 06.08.2015.
 */

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.util.{Saveable}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/** Create LSH model for maximum m number of elements in each vector.
  *
  * @param m max number of possible elements in a vector
  * @param numHashFunc number of hash functions
  * @param numHashTables number of hash tables.
  *
  * */
class LSHModel(val m: Int, val numHashFunc : Int, val numHashTables: Int)
  extends Serializable with Saveable {

  /** generate numHashFunc * numHashTables randomly generated hash functions and store them in hashFunctions */
  private val _hashFunctions = ListBuffer[Hasher]()
  for (i <- 0 until numHashFunc * numHashTables)
    _hashFunctions += Hasher(m)
  final var hashFunctions: List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

  /** the "hashTables" ((hashTableID, hash key), vector_id) */
  var hashTables: RDD[((Int, String), Long)] = null

  /** generic filter function for hashTables. */
  def filter(f: (((Int, String), Long)) => Boolean): RDD[((Int, String), Long)] =
    hashTables.map(a => a).filter(f)

  /** hash a single vector against an existing model and return the candidate buckets */
  def filter(data: SparseVector, model: LSHModel, itemID: Long): RDD[Long] = {
    val hashKey = hashFunctions.map(h => h._1.hash(data)).mkString("")
    hashTables.filter(x => x._1._2 == hashKey).map(a => a._2)
  }

  /** creates hashValue for each hashTable.*/
  def hashValue(data: SparseVector): List[(Int, String)] =
    hashFunctions.map(a => (a._2 % numHashTables, a._1.hash(data)))
    .groupBy(_._1)
    .map(x => (x._1, x._2.map(_._2).mkString(""))).toList

  /** returns candidate set for given vector id.*/
  def getCandidates(vId: Long): RDD[Long] = {
    val buckets = hashTables.filter(x => x._2 == vId).map(x => x._1).distinct().collect()
    hashTables.filter(x => buckets contains x._1).map(x => x._2).filter(x => x != vId)
  }

  /** returns candidate set for given vector.*/
  def getCandidates(v: SparseVector): RDD[Long] = {
    val hashVal = hashValue(v)
    hashTables.filter(x => hashVal contains x._1).map(x => x._2)
  }

  /** adds a new sparse vector with vector Id: vId to the model. */
  def add (vId: Long, v: SparseVector, sc: SparkContext): LSHModel = {
    val newRDD = sc.parallelize(hashValue(v).map(a => (a, vId)))
    hashTables ++ newRDD
    this
  }

  /** remove sparse vector with vector Id: vId from the model. */
  def remove (vId: Long, sc: SparkContext): LSHModel = {
    hashTables =  hashTables.filter(x => x._2 != vId)
    this
  }

  override def save(sc: SparkContext, path: String): Unit =
    LSHModel.SaveLoadV0_0_1.save(sc, this, path)

  override protected def formatVersion: String = "0.0.1"

}

object LSHModel {

  def load(sc: SparkContext, path: String): LSHModel = {
    LSHModel.SaveLoadV0_0_1.load(sc, path)
  }

  private [lsh] object SaveLoadV0_0_1 {

    private val thisFormatVersion = "0.0.1"
    private val thisClassName = this.getClass.getName()

    def save(sc: SparkContext, model: LSHModel, path: String): Unit = {

      val metadata =
        compact(render(("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))

      //save metadata info
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      //save hash functions as (hashTableId, randomVector)
      sc.parallelize(model.hashFunctions
        .map(x => (x._2, x._1.r.mkString(",")))
        .map(_.productIterator.mkString(",")))
        .saveAsTextFile(Loader.hasherPath(path))

     //save data as (hashTableId#, hashValue, vectorId)
      model.hashTables
        .map(x => (x._1._1, x._1._2, x._2))
        .map(_.productIterator.mkString(","))
        .saveAsTextFile(Loader.dataPath(path))

    }

    def load(sc: SparkContext, path: String): LSHModel = {

      implicit val formats = DefaultFormats
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val hashTables = sc.textFile(Loader.dataPath(path))
        .map(x => x.split(","))
        .map(x => ((x(0).toInt, x(1)), x(2).toLong))
      val hashers = sc.textFile(Loader.hasherPath(path))
        .map(a => a.split(","))
        .map(x => (x.head, x.tail))
        .map(x => (new Hasher(x._2.map(_.toDouble)), x._1.toInt)).collect().toList
      val numHashTables = hashTables.map(x => x._1._1).distinct.count()
      val numHashFunc = hashers.size / numHashTables

      //Validate loaded data
      //check size of data
      assert(hashTables.count != 0, s"Loaded hashTable data is empty")
      //check size of hash functions
      assert(hashers.size != 0, s"Loaded hasher data is empty")
      //check hashValue size. Should be equal to numHashFunc
      assert(hashTables.map(x => x._1._2).filter(x => x.size != numHashFunc).collect().size == 0,
        s"hashValues in data does not match with hash functions")

      //create model
      val model = new LSHModel(0, numHashFunc.toInt, numHashTables.toInt)
      model.hashFunctions = hashers
      model.hashTables = hashTables

      model
    }
  }
}


/** Helper functions for save/load data from mllib package.
  * TODO: Remove and use Loader functions from mllib. */
private[lsh] object Loader {

  /** Returns URI for path/data using the Hadoop filesystem */
  def dataPath(path: String): String = new Path(path, "data").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def hasherPath(path: String): String = new Path(path, "hasher").toUri.toString

  /**
   * Load metadata from the given path.
   * @return (class name, version, metadata)
   */
  def loadMetadata(sc: SparkContext, path: String): (String, String, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    (clazz, version, metadata)
  }

}