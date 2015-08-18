package com.lendap.lsh

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

/**
 * Created by maytekin on 05.08.2015.
 */
object Main {

  def main(args: Array[String]) {
    val numPartitions = 8
    val dataFile = "data/ml-1m.data"
    val conf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    //read data file in as a RDD, partition RDD across <partitions> cores
    val data = sc.textFile(dataFile)
    val ratingsRDD = data
      .map(line => line.split("::"))
      .map(elems => Rating(elems(0).toInt, elems(1).toInt, elems(2).toDouble))

    val users = ratingsRDD.map(ratings => ratings.user).distinct()
    val items = ratingsRDD.map(ratings => ratings.product).distinct()
    val maxElem = items.max + 1
    println(maxElem)
    val ratings50 = ratingsRDD.map(a => (a.user, (a.product, a.rating))).groupByKey().filter(a=>a._2.size > 50)
    val mostRatedMovies = ratingsRDD.map(a => a.product).countByValue.toSeq
    val userRatings = ratingsRDD.map(a => (a.user, (a.product, a.rating))).groupByKey()
    val sampleRating = userRatings.take(1)(0)._2.toSeq
    val spData = userRatings.map(a=>(a._1.toLong, Vectors.sparse(maxElem, a._2.toSeq).asInstanceOf[SparseVector]))

    println(users.count() + " users rated on " +
      items.count() + " movies and "  +
      ratings50.count() + " users have more than 50 ratings.")
    val numHashFunc = 8

    val size = items.count()
    //run locality sensitive hashing
    val lsh = new  LSH(spData, maxElem, 6, 6)
    val model = lsh.model

    val h = Hasher.create(maxElem)

    //val (user, vec) = sampleUserVec(0)
    //print(h.hash(vec.asInstanceOf[SparseVector]))
    //model.bands.collect() foreach println
    model.filter("100010") foreach println
  }

}
