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
      .map(line => line.split("\t"))
      .map(elems => (Rating(elems(0).toInt, elems(1).toInt, elems(2).toDouble)))
    val users = ratingsRDD.map(ratings => ratings.user).distinct()
    val items = ratingsRDD.map(ratings => ratings.product).distinct()
    val ratings50 = ratingsRDD.map(a => (a.user, (a.product, a.rating))).groupByKey().filter(a=>a._2.size > 50)
    val mostRatedMovies = ratingsRDD.map(a => a.product).countByValue.toSeq
    println(mostRatedMovies.sortBy(-_._2).take(50).toList)
    //val rating  = ratingsRDD.filter(a => a.user == 4904 && a.product == 2054)
    println(users.count() + " users rated on " +
      items.count() + " movies and "  +
      ratings50.count() + " users have more than 50 ratings.")
    val numHashFunc = 8
    val sampleUser = ratings50.take(1)

    val (indices, values) = sampleUser(0)._2.toSeq.sortBy(_._1).unzip
    println(indices.size)
    println(values.size)

    val sampleUserVec = sampleUser.map(a=>(a._1, Vectors.sparse(a._2.size, indices.toArray, values.toArray)))
    //val m = 100 /** number of elements */
    val maxElem = items.max()
    val size = items.count()

    val h = Hasher.create(maxElem, sc, numPartitions)

    //val i = Array(0, 2, 3, 6)
    //val v = Array(1.0, -4.0, 6.0, -20.0)
    val (user, vec) = sampleUserVec(0)
    print(h.hash(vec.asInstanceOf[SparseVector]))
    println("")
  }

}
